/*
Borg Repository Manager

Will use borg container:
* FindRepository
* Setup
* Info
* Contents

Uses local borg installation:
* Delete
* Prune
*/
package borg

import (
	"cs-agent/types"
	"github.com/spf13/viper"
	"reflect"
	"strconv"

	"github.com/getsentry/sentry-go"
)

func FindRepository(vol *types.Volume, source *types.Volume) (*Repository, *LogMessage) {

	r := Repository{Name: vol.Name, Retention: vol.Retention, SourceVolumeName: source.Name}

	containerBuilt, containerErr := r.InitBackupContainer(vol, source)
	if containerErr != nil {
		sentry.CaptureException(containerErr)
		return nil, &LogMessage{Message: containerErr.Error()}
	}
	if !containerBuilt {
		return nil, &LogMessage{Message: "Failed to build backup container"}
	}

	// Find Repo
	repoResponse, err := r.Info()

	if err != nil {
		r.StopContainer()
		return nil, err
	}

	if repoResponse == (RepositoryResponse{}) {
		r.StopContainer()
		return nil, &LogMessage{MsgID: "Repository.DoesNotExist", Message: "Missing Repository"}
	}

	return &r, nil
}

func (r *Repository) FindArchive(name string) (a *Archive, err *LogMessage) {
	a = &Archive{Name: name, Repository: r}
	// Attempt to load archive. Nil = not exist.
	if _, err = a.Info(); err != nil {
		return nil, err
	}
	return a, nil
}

func (r *Repository) Setup(vol *types.Volume, source *types.Volume) *LogMessage {

	if reflect.ValueOf(r.Container).IsNil() {
		containerBuilt, containerErr := r.InitBackupContainer(vol, source)
		if containerErr != nil {
			sentry.CaptureException(containerErr)
			return &LogMessage{Message: containerErr.Error()}
		}
		if !containerBuilt {
			return &LogMessage{Message: "Failed to build backup container"}
		}
	}

	var backupCmd []string

	backupCmd = append(backupCmd, "borg --log-json")
	backupCmd = append(backupCmd, "--lock-wait "+viper.GetString("backups.borg.lock_wait"))
	backupCmd = append(backupCmd, "init --error --encryption=repokey-blake2")

	if _, _, log := r.ExecWithLog(backupCmd); log != (LogMessage{}) {
		return &log
	}

	consulRepo := ConsulRepository{Name: r.Name}
	consulErr := consulRepo.Save()
	if consulErr != nil {
		borgLogger().Warn("Fatal error saving data to Consul", "repository", r.Name, "error", consulErr.Error())
		sentry.CaptureException(consulErr)
	}
	return nil
}

func (r *Repository) Info() (RepositoryResponse, *LogMessage) {
	if reflect.ValueOf(r.Container).IsNil() {
		return RepositoryResponse{}, &LogMessage{Message: "Missing backup container"}
	}

	cmd := []string{"borg --log-json"}
	cmd = append(cmd, "--lock-wait "+viper.GetString("backups.borg.lock_wait"))
	cmd = append(cmd, "info --error --json")

	_, response, logMsg := r.ExecWithLog(cmd)

	if logMsg != (LogMessage{}) {
		return RepositoryResponse{}, &logMsg
	}

	repoResponse, repoLog := readRepoResponse(response)

	if repoLog != nil {
		return RepositoryResponse{}, repoLog
	}

	return repoResponse, nil
}

func (r *Repository) Contents() (RepositoryContentResponse, *LogMessage) {
	if reflect.ValueOf(r.Container).IsNil() {
		return RepositoryContentResponse{}, &LogMessage{Message: "Missing backup container"}
	}

	cmd := []string{"borg --log-json"}
	cmd = append(cmd, "--lock-wait "+viper.GetString("backups.borg.lock_wait"))
	cmd = append(cmd, "list --error --json")

	_, response, logMsg := r.ExecWithLog(cmd)

	if logMsg != (LogMessage{}) {
		return RepositoryContentResponse{}, &logMsg
	}

	repoResponse, repoLog := readRepoContentResponse(response)

	if repoLog != nil {
		return RepositoryContentResponse{}, repoLog
	}

	return repoResponse, nil
}

func (r *Repository) Delete() (bool, error) {
	vol := types.Volume{Name: r.Name, Trash: true}
	return r.TrashBackupVolumeExists(&vol)
}

/*
*

		Prune Repository

		*  Will ignore all repositories that don't match the `auto-` prefix.
	    *  Testing this by creating 2 backups back-to-back, and then running prune with
		   an hourly retention of 2 will only retain 1 because the content would not have changed between the 2 backups.
*/
func (r *Repository) Prune() *LogMessage {
	vol := types.Volume{Name: r.Name, Trash: true}
	sourceVol := types.Volume{Name: r.SourceVolumeName, Trash: true}
	if reflect.ValueOf(r.Container).IsNil() {
		containerBuilt, containerErr := r.InitBackupContainer(&vol, &sourceVol)
		if containerErr != nil {
			sentry.CaptureException(containerErr)
			return &LogMessage{Message: containerErr.Error()}
		}
		if !containerBuilt {
			return &LogMessage{Message: "Failed to build backup container"}
		}
	}

	cmd := []string{"borg --log-json"}
	cmd = append(cmd, "--lock-wait "+viper.GetString("backups.borg.lock_wait"))
	cmd = append(cmd, "prune --error --stats --prefix=\"auto-\"")
	cmd = append(cmd, "--keep-hourly="+strconv.Itoa(r.Retention.Hourly))
	cmd = append(cmd, "--keep-daily="+strconv.Itoa(r.Retention.Daily))
	cmd = append(cmd, "--keep-weekly="+strconv.Itoa(r.Retention.Weekly))
	cmd = append(cmd, "--keep-monthly="+strconv.Itoa(r.Retention.Monthly))
	cmd = append(cmd, "--keep-yearly="+strconv.Itoa(r.Retention.Annually))

	if _, _, log := r.ExecWithLog(cmd); log != (LogMessage{}) {
		return &log
	}

	r.SyncConsul()
	borgLogger().Info("Completed prune event", "volume_name", r.Name)
	return nil
}

func (r *Repository) SyncConsul() {
	if reflect.ValueOf(r.Container).IsNil() {
		return
	}
	consulRepo, consulRepoErr := FindConsulRepo(r.Name)

	if consulRepoErr != nil {
		borgLogger().Error("Error during SyncConsul", "function", "FindConsulRepo", "error", consulRepoErr.Error())
		return
	}

	if consulRepo == nil {
		borgLogger().Error("Error during SyncConsul", "function", "FindConsulRepo", "error", "unable to find consulRepo")
		return
	}

	// get list of archives
	contents, contentsErr := r.Contents()

	if contentsErr != nil {
		borgLogger().Error("Error during SyncConsul", "function", "Repository.Contents()", "errorCode", contentsErr.MsgID, "error", contentsErr.Message)
		return
	}

	consulRepo.Archives = []string{} // start with a fresh, and empty, archive list

	for _, item := range contents.Archives {
		consulRepo.Archives = append(consulRepo.Archives, item.Name)
	}

	repoInfo, repoInfoErr := r.Info()

	if repoInfoErr != nil {
		borgLogger().Error("Error during SyncConsul", "function", "Repository.Info()", "errorCode", repoInfoErr.MsgID, "error", repoInfoErr.Message)
		return
	}

	consulRepo.SizeOnDisk = repoInfo.Cache.Stats.UniqueCSize
	consulRepo.TotalSize = repoInfo.Cache.Stats.TotalCSize

	saveErr := consulRepo.Save()

	if saveErr != nil {
		borgLogger().Error("Failed to save data to consul", "function", "consulRepo.Save()", "error", saveErr.Error())
		sentry.CaptureException(saveErr)

	}
}

func (r *Repository) repoPath() string {
	if viper.GetBool("backups.borg.ssh.enabled") {
		sshUser := viper.GetString("backups.borg.ssh.user")
		sshHost := viper.GetString("backups.borg.ssh.host")
		sshPort := viper.GetString("backups.borg.ssh.port")
		hostPath := viper.GetString("backups.borg.ssh.host_path")
		fullPath := "ssh://" + sshUser + "@" + sshHost + ":" + sshPort + hostPath + "/b-" + r.Name + "/backup"
		return fullPath
	} else {
		return "/mnt/borg/backup"
	}
}
