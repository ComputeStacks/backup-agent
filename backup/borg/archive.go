package borg

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/hashicorp/go-hclog"
	"github.com/spf13/viper"
)

func (a *Archive) Create() (ArchiveMessage, *LogMessage) {
	var borgResponse ArchiveMessage
	var log LogMessage
	if a.Repository == nil {
		log.Message = "Missing Repository"
		return borgResponse, &log
	}

	if reflect.ValueOf(a.Repository.Container).IsNil() {
		return borgResponse, &LogMessage{Message: "Missing backup container"}
	}

	if !a.generateName() {
		log.Message = "Unable to generate unique archive name"
		return borgResponse, &log
	}

	backupCmd := []string{"cd /mnt/data && borg --log-json create --error --one-file-system --json --numeric-ids --exclude-caches"}
	backupCmd = append(backupCmd, "--compression "+viper.GetString("backups.borg.compression"))
	backupCmd = append(backupCmd, a.archivePath())
	backupCmd = append(backupCmd, ".")

	_, response, log := a.Repository.ExecWithLog(backupCmd)

	if log != (LogMessage{}) {
		return ArchiveMessage{}, &log
	}
	a.Repository.SyncConsul()
	marshalErr := json.Unmarshal([]byte(response), &borgResponse)
	if marshalErr == nil {
		borgLogger().Info("Completed backup", "archive", borgResponse.Archive.ID, "duration", hclog.Fmt("%.5f", borgResponse.Archive.Duration))
		return borgResponse, nil
	} else {
		borgLogger().Debug("Unmarshal Error on Borg Backup Response", "error", marshalErr.Error(), "raw", response)
	}
	borgLogger().Warn("Backup appears to have succeeded, but there was an error decoding the response data from borg.")
	return ArchiveMessage{}, nil
}

/**
 * Restore an archive to a volume
 *
 * (DEPRECATED) You can optionally specify specific files (including their path) to restore. Otherwise, it will restore the entire directory.
 */
func (a *Archive) Restore(filePaths []string) *LogMessage {
	if reflect.ValueOf(a.Repository.Container).IsNil() {
		return &LogMessage{Message: "Missing backup container"}
	}

	// Move current structure to snapshot
	preRestore := []string{"mkdir", "-p /root/.snapshot"}
	preRestore = append(preRestore, "&&", "mv", "/mnt/data/* /root/.snapshot/")

	_, _, preRestoreLog := a.Repository.ExecWithLog(preRestore)

	if preRestoreLog != (LogMessage{}) {
		return &preRestoreLog
	}

	// Perform Restore
	cmd := []string{"cd /mnt/data && borg --log-json extract --error --numeric-ids"}
	cmd = append(cmd, a.archivePath())
	for _, p := range filePaths {
		cmd = append(cmd, p)
	}
	_, response, log := a.Repository.ExecWithLog(cmd)

	borgLogger().Debug("Restore Response", "output", response)

	if log != (LogMessage{}) {

		// Failed, so we roll back
		rollbackCmd := []string{"rm", "-rf /mnt/data/*"}
		rollbackCmd = append(rollbackCmd, "&&", "mv /root/.snapshot/* /mnt/data/")
		if _, rollbackResponse, rollbackLog := a.Repository.ExecWithLog(rollbackCmd); rollbackLog != (LogMessage{}) {
			borgLogger().Warn("Fatal error performing rollback on restore", "response", rollbackResponse, "error", rollbackLog.Message)
		}
		return &log
	}
	return readArchiveRestoreResponse(response)
}

func (a *Archive) Info() (*ArchiveResponse, *LogMessage) {
	if reflect.ValueOf(a.Repository.Container).IsNil() {
		return &ArchiveResponse{}, &LogMessage{Message: "Missing backup container"}
	}
	var archiveResponse ArchiveResponse

	cmd := []string{"borg --log-json info --error --json"}
	cmd = append(cmd, a.archivePath())

	_, response, log := a.Repository.ExecWithLog(cmd)

	if log != (LogMessage{}) {
		return nil, &log
	}

	marshalErr := json.Unmarshal([]byte(response), &archiveResponse)

	if marshalErr != nil {
		log.Message = marshalErr.Error()
		sentry.CaptureException(marshalErr)
		borgLogger().Error("Error unmarshaling json", "function", "Archive.Info", "error", marshalErr.Error())
		return nil, &log
	}

	return &archiveResponse, nil
}

func (a *Archive) Delete() ([]LogMessage, *LogMessage) {
	var results []LogMessage

	if reflect.ValueOf(a.Repository.Container).IsNil() {
		return results, &LogMessage{Message: "Missing backup container"}
	}

	cmd := []string{"borg --log-json --error delete --stats --force"}
	cmd = append(cmd, a.archivePath())

	borgLogger().Debug("Raw Delete Command", "cmd", strings.Join(cmd, " "))

	exitCode, response, log := a.Repository.ExecWithLog(cmd)

	if exitCode > 0 {
		log.Message = "Did not exit correctly. Response: " + response
		return results, &log
	}

	if response == "" || (log != (LogMessage{})) {
		return results, &log
	}

	list := strings.Split(response, "\n")
	for _, d := range list {
		var result LogMessage
		if jErr := json.Unmarshal([]byte(d), &result); jErr != nil {
			continue
		}
		results = append(results, result)
	}
	a.Repository.SyncConsul()

	borgLogger().Info("Completed Archive Delete event", "volume", a.Repository.Name, "archive", a.Name)
	return results, nil
}

func (a *Archive) generateName() bool {
	if a.Repository == nil {
		return false
	}
	contents, contentsErr := a.Repository.Contents()
	if contentsErr != nil {
		return false
	}
	rand.New(rand.NewSource(time.Now().UnixNano()))
	randNum := rand.Intn(10000-10) + 10
	for _, i := range contents.Archives {
		if i.Name == a.Name {
			a.Name = a.Name + "-" + strconv.Itoa(randNum)
			break
		}
	}
	return a.Name != ""
}

func (a *Archive) archivePath() string {
	return "::" + a.Name
	//return a.Repository.repoPath() + "::" + a.Name
}
