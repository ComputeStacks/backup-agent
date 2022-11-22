package borg

import (
	"context"
	"cs-agent/containermgr"
	"cs-agent/sshremote"
	"cs-agent/types"
	"encoding/json"
	"errors"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

	dockerTypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	volumeTypes "github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/getsentry/sentry-go"
	"github.com/spf13/viper"
)

func (r *Repository) InitBackupContainer(vol *types.Volume, source *types.Volume) (bool, error) {
	if !reflect.ValueOf(r.Container).IsNil() {
		// If a container already exists, stop.
		return true, nil
	}
	cli, clientErr := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if clientErr != nil {
		borgLogger().Error("Unable to connect to Docker", "error", clientErr.Error())
		return false, clientErr
	}

	// Check if the backup volume exists, and create it if it does not.
	if _, volErr := r.ensureBackupVolumeExists(cli, source); volErr != nil {
		return false, volErr
	}

	ctx := context.Background()

	// Ensure image exists
	_, _, missingImage := cli.ImageInspectWithRaw(ctx, viper.GetString("backups.borg.image"))
	if missingImage != nil {
		_, err := cli.ImagePull(ctx, viper.GetString("backups.borg.image"), dockerTypes.ImagePullOptions{})
		if err != nil {
			borgLogger().Error("Fatal error pulling image", "error", clientErr.Error())
			return false, err
		}
	}

	// Container Labels
	labels := make(map[string]string)
	labels["com.computestacks.role"] = "backup"
	labels["com.computestacks.for"] = vol.Name

	// Generate Container Name
	t := time.Now()
	rand.Seed(time.Now().UnixNano()) // Seed for random container name
	randNumber := 10 + rand.Intn(1000-10)
	containerName := "backup-" + strconv.Itoa(randNumber) + string(t.Format("150405"))

	hostConfig := container.HostConfig{
		NetworkMode: "none",
		Binds:       []string{},
		Mounts:      []mount.Mount{},
		AutoRemove:  true,
		Privileged:  viper.GetBool("docker.privileged"),
	}

	hostConfig.Mounts = append(hostConfig.Mounts, mount.Mount{
		Type:   mount.TypeVolume,
		Source: "b-" + source.Name,
		Target: "/mnt/borg",
	})

	if !vol.Trash {
		hostConfig.Mounts = append(hostConfig.Mounts, mount.Mount{
			Type:   mount.TypeVolume,
			Source: vol.Name,
			Target: "/mnt/data",
		})
	}

	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image:  viper.GetString("backups.borg.image"),
		Labels: labels,
		Env: []string{
			"BORG_PASSPHRASE=" + viper.GetString("backups.key"),
			"BORG_RELOCATED_REPO_ACCESS_IS_OK=yes",
			"BORG_DELETE_I_KNOW_WHAT_I_AM_DOING=YES",
			"BORG_CHECK_I_KNOW_WHAT_I_AM_DOING=YES",
			"BORG_BASE_DIR=/mnt/borg",
		},
	}, &hostConfig, nil, containerName)

	if err != nil {
		return false, err
	}

	if err := cli.ContainerStart(ctx, resp.ID, dockerTypes.ContainerStartOptions{}); err != nil {
		return false, err
	}

	r.Container = &containermgr.Container{ID: resp.ID}

	// time.Sleep(250 * time.Millisecond)
	isReady := false
	for counter := 1; counter < 12; counter++ {
		c, errRunning := cli.ContainerInspect(ctx, resp.ID)
		if errRunning == nil && c.State.Running {
			isReady = true
			break
		} else if errRunning == nil {
			borgLogger().Debug("Waiting for container before executing command", "container", c.ID, "state", c.State.Status)
		} else {
			borgLogger().Debug("Waiting for container before executing command", "error", errRunning.Error())
			// For fatal errors, reduce the total count to 3 instead of 12.
			if counter > 2 {
				break
			}
		}
		time.Sleep(time.Second)
	}
	if !isReady {
		return false, errors.New("container never came online")
	}

	return true, nil
}

func (r *Repository) ExecWithLog(cmd []string) (exitCode int, response string, log LogMessage) {
	if reflect.ValueOf(r.Container).IsNil() {
		return 99, "", LogMessage{Message: "Missing backup container"}
	}
	execCmd := []string{"ash", "-c", strings.Join(cmd, " ")}
	exitCode, response, err := r.Container.Exec(execCmd, nil)

	if err != nil {
		borgLogger().Debug("ExecWithLog Error", "error", err.Error())
		if response == "" {
			log.Message = err.Error()
			borgLogger().Error("Fatal ExecWithLog", "error", err.Error(), "repo", r.Name)
			sentry.CaptureException(err)
			return exitCode, response, log
		}
		if strings.Contains(response, "}\n{") {
			response = strings.Split(response, "\n{")[0]
		}
		jsonErr := json.Unmarshal([]byte(response), &log)
		if jsonErr != nil {
			log.Message = jsonErr.Error()
			return exitCode, response, log
		}
		borgLogger().Warn("Error", "msgid", log.MsgID, "message", log.Message)
		return exitCode, response, log
	}

	return exitCode, response, log

}

/*
 * Helper methods to deal with situations where the container is null.
 */

// StopContainer will stop the backup container.
func (r *Repository) StopContainer() bool {
	if r.Container == nil {
		return true
	}
	return r.Container.Stop()
}

/*
Volumes
*/
func (r *Repository) ensureBackupVolumeExists(cli *client.Client, vol *types.Volume) (bool, error) {
	ctx := context.Background()

	_, existingVolumeErr := cli.VolumeInspect(ctx, "b-"+vol.Name)

	if existingVolumeErr != nil {
		// Container Labels
		labels := make(map[string]string)
		labels["com.computestacks.role"] = "backup"
		labels["com.computestacks.for"] = vol.Name

		// Driver Opts
		driverOpts := make(map[string]string)

		if viper.GetBool("backups.borg.nfs") {
			if viper.GetBool("backups.borg.nfs_create_path") {
				borgLogger().Info("Creating remote volume directory", "volume", "b-"+vol.Name)
				sshCmd := "mkdir -p " + viper.GetString("backups.borg.nfs_host_path") + "/b-" + vol.Name
				sshCmd = sshCmd + " && chown -R " + viper.GetString("backups.borg.fs.user") + ":" + viper.GetString("backups.borg.fs.group") + " " + viper.GetString("backups.borg.nfs_host_path") + "/b-" + vol.Name
				connInfo := sshremote.ServerConnInfo{
					Server: viper.GetString("backups.borg.nfs_host"),
					Port:   viper.GetString("backups.borg.nfs_ssh.port"),
					User:   viper.GetString("backups.borg.nfs_ssh.user"),
					Key:    viper.GetString("backups.borg.nfs_ssh.keyfile"),
				}

				createDirSuccess, createDirErr := sshremote.SSHCommandBool(sshCmd, connInfo)

				if createDirErr != nil {
					borgLogger().Error("Fatal error creating directory on remote server", "volume", "b-"+vol.Name, "error", createDirErr.Error())
					return false, createDirErr
				}

				if !createDirSuccess {
					borgLogger().Warn("Invalid response while creating remote directory", "volume", "b-"+vol.Name)
					return false, errors.New("invalid response while creating directory")
				}
			}
			driverOpts["type"] = "nfs"
			driverOpts["o"] = "addr=" + viper.GetString("backups.borg.nfs_host") + ",rw,nfsvers=4" + viper.GetString("backups.borg.nfs_opts")
			driverOpts["device"] = ":" + viper.GetString("backups.borg.nfs_host_path") + "/b-" + vol.Name
		}

		opts := volumeTypes.VolumeCreateBody{
			Name:       "b-" + vol.Name,
			Driver:     "local",
			DriverOpts: driverOpts,
			Labels:     labels,
		}

		_, volErr := cli.VolumeCreate(ctx, opts)

		if volErr != nil {
			borgLogger().Warn("Fatal Error Creating Volume", "volume", "b-"+vol.Name, "error", volErr.Error())
			return false, volErr
		}

		return true, nil

	}
	return true, nil
}

func (r *Repository) TrashBackupVolumeExists(vol *types.Volume) (bool, error) {
	ctx := context.Background()
	cli, clientErr := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if clientErr != nil {
		borgLogger().Error("Unable to connect to Docker", "error", clientErr.Error())
		return false, clientErr
	}
	existingVolume, existingVolumeErr := cli.VolumeInspect(ctx, "b-"+vol.Name)

	if existingVolumeErr != nil {
		borgLogger().Info("Volume does not exist, skipping...")
		return true, nil
	}
	if removeErr := cli.VolumeRemove(ctx, existingVolume.Name, false); removeErr != nil {
		borgLogger().Error("Error Deleting Volume", "error", removeErr.Error())
		return false, removeErr
	}

	if viper.GetBool("backups.borg.nfs") {
		borgLogger().Info("Cleaning remote volume path", "volume", "b-"+vol.Name)

		sshCmd := "rm -rf " + viper.GetString("backups.borg.nfs_host_path") + "/b-" + vol.Name
		connInfo := sshremote.ServerConnInfo{
			Server: viper.GetString("backups.borg.nfs_host"),
			Port:   viper.GetString("backups.borg.nfs_ssh.port"),
			User:   viper.GetString("backups.borg.nfs_ssh.user"),
			Key:    viper.GetString("backups.borg.nfs_ssh.keyfile"),
		}

		destroyDirSuccess, destroyDirErr := sshremote.SSHCommandBool(sshCmd, connInfo)

		if destroyDirErr != nil {
			borgLogger().Error("Error removing remote directory", "volume", "b-"+vol.Name, "error", destroyDirErr.Error())
			return false, destroyDirErr
		}

		if !destroyDirSuccess {
			borgLogger().Warn("Invalid response while destroying remote directory", "volume", "b-"+vol.Name)
			return false, errors.New("invalid response while destroying directory")
		}
	} else {
		borgLogger().Info("NFS disabled, skipping remote file cleanup.")
	}

	borgLogger().Info("Successfully removed backup volume", "volume", "b-"+vol.Name)
	return true, nil
}
