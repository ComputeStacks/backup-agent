package containermgr

import (
	"bytes"
	"context"
	"cs-agent/csevent"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	dockerTypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/spf13/viper"
)

type Container struct {
	ID string
}

func FindByService(cli *client.Client, serviceID string, allowOff bool) (*Container, error) {
	containers, err := FindAllByService(cli, serviceID, allowOff)

	if err != nil {
		containerLogger().Debug("FindContainerByService: Error", "error", err.Error())
		return nil, err
	}

	containerLogger().Debug("FindRunningContainer: Found Containers", "containers", strconv.Itoa(len(containers)))

	if len(containers) < 1 {
		return nil, errors.New("no containers found")
	}

	return containers[0], nil

}

func FindAllByService(cli *client.Client, serviceID string, allowOff bool) (containers []*Container, err error) {
	ctx := context.Background()
	findArgs := filters.NewArgs(
		filters.Arg("label", "com.computestacks.service_id="+serviceID),
	)
	findOpts := dockerTypes.ContainerListOptions{
		All:     true,
		Filters: findArgs,
	}
	findContainer, err := cli.ContainerList(ctx, findOpts)

	if err != nil {
		return containers, err
	}

	skipContainer := false

	for _, c := range findContainer {
		containerLogger().Debug("Looking for container", "container", c.ID, "state", c.State)
		if allowOff {
			for k, v := range c.Labels {
				if k == "com.computestacks.role" {
					if v == "backup" || v == "bastion" {
						skipContainer = true
					}
				}
			}
			if !skipContainer {
				newContainer := Container{ID: c.ID}
				containers = append(containers, &newContainer)
			}
		} else {
			if c.State == "running" {
				for k, v := range c.Labels {
					if k == "com.computestacks.role" {
						if v == "backup" || v == "bastion" {
							skipContainer = true
						}
					}
				}
				if !skipContainer {
					newContainer := Container{ID: c.ID}
					containers = append(containers, &newContainer)
				}
			}
		}
		skipContainer = false
	}

	return containers, nil
}

// Helper to exec inside a container when you don't specifically know the container ID
func ServiceExec(serviceID string, jobCommands []string, event *csevent.ProjectEvent) (int, string, error) {
	cli, err := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if err != nil {
		return 1, "", err
	}

	c, err := FindByService(cli, serviceID, false)

	if err != nil {
		go event.PostEventUpdate("agent-9ae526db94f38e41", "Failed to locate running container")
		return 1, "", err
	}
	return c.Exec(jobCommands, event)
}

func (c *Container) Stop() bool {
	if c == nil {
		return true
	}
	cli, err := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if err != nil {
		containerLogger().Warn("Failed to connect to docker daemon", "error", err.Error(), "function", "ContainerStop")
		return false
	}
	ctx := context.Background()
	timeout := time.Duration(15)
	if containerStopError := cli.ContainerStop(ctx, c.ID, &timeout); containerStopError != nil {
		if strings.Contains(containerStopError.Error(), "no such container") {
			return true
		}
		containerLogger().Warn("Error stopping backup container", "error", containerStopError.Error(), "backupContainerID", c.ID)
		return false
	}

	// Ensure container actually stops.
	isStopped := false
	for counter := 1; counter < 12; counter++ {
		dockerContainer, errRunning := cli.ContainerInspect(ctx, c.ID)
		if errRunning != nil { // if container does not exist, an error is returned.
			isStopped = true
			break
		} else if !dockerContainer.State.Running {
			isStopped = true
			break
		}
		containerLogger().Debug("Waiting for container to stop", "container", dockerContainer.ID, "state", dockerContainer.State.Status)
		time.Sleep(500 * time.Millisecond)
	}
	return isStopped
}

func (c *Container) Start() bool {
	if c == nil {
		return true
	}
	cli, err := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if err != nil {
		containerLogger().Warn("Failed to connect to docker daemon", "error", err.Error(), "function", "ContainerStart")
		return false
	}
	ctx := context.Background()
	if containerStartError := cli.ContainerStart(ctx, c.ID, dockerTypes.ContainerStartOptions{}); containerStartError != nil {
		containerLogger().Warn("Error starting backup container", "error", containerStartError.Error(), "backupContainerID", c.ID)
		return false
	}

	// Ensure container actually stops.
	isStarted := false
	for counter := 1; counter < 12; counter++ {
		dockerContainer, errRunning := cli.ContainerInspect(ctx, c.ID)
		if errRunning == nil && dockerContainer.State.Running { // if container does not exist, an error is returned.
			isStarted = true
			break
		} else if errRunning == nil {
			containerLogger().Debug("Waiting for container to start", "container", dockerContainer.ID, "state", dockerContainer.State.Status)
		} else {
			containerLogger().Debug("Fatal error while trying to start container", "error", errRunning.Error())
			if counter > 2 {
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return isStarted
}

func (c *Container) Exec(jobCommands []string, event *csevent.ProjectEvent) (exitCode int, response string, err error) {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.WithVersion(viper.GetString("docker.version")))
	if err != nil {
		return 1, "", err
	}
	isReady := false

	for counter := 1; counter < 12; counter++ {
		dockerContainer, errRunning := cli.ContainerInspect(ctx, c.ID)
		if errRunning == nil && dockerContainer.State.Running {
			isReady = true
			break
		} else if errRunning == nil {
			containerLogger().Debug("Waiting for container before executing command", "container", dockerContainer.ID, "state", dockerContainer.State.Status)
		} else {
			containerLogger().Debug("Waiting for container before executing command", "error", errRunning.Error())
			// For fatal errors, reduce the total count to 3 instead of 12.
			if counter > 2 {
				break
			}
		}
		time.Sleep(time.Second)
	}

	if !isReady {
		return 1, "", errors.New("container never came online")
	}

	execConfig := dockerTypes.ExecConfig{
		Cmd:          jobCommands,
		Tty:          true,
		AttachStderr: true,
		AttachStdout: true,
		AttachStdin:  true,
		Detach:       false,
	}
	execResponse, err := cli.ContainerExecCreate(ctx, c.ID, execConfig)

	if err != nil {
		return 1, "", err
	}

	execStartCheck := dockerTypes.ExecStartCheck{
		Tty: execConfig.Tty,
	}

	resp, err := cli.ContainerExecAttach(ctx, execResponse.ID, execStartCheck)
	if err != nil {
		return 1, "", err
	}
	defer resp.Close()

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Reader)
	s := buf.String()

	if !reflect.ValueOf(event).IsNil() {
		go event.PostEventUpdate("agent-53a6ba4b3dc92e0f", s)
	}

	respStatus, err := cli.ContainerExecInspect(ctx, execResponse.ID)

	if err != nil {
		return 1, s, err
	}

	return respStatus.ExitCode, s, nil
}
