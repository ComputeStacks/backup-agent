package backup

import (
	"context"
	"cs-agent/containermgr"
	"cs-agent/csevent"
	"cs-agent/types"
	"errors"
	"github.com/getsentry/sentry-go"
	"math/rand"
	"strconv"
	"strings"
	"time"

	dockerTypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/shopspring/decimal"
)

type MysqlInstance struct {
	Name         string
	Container    *containermgr.Container
	Version      decimal.Decimal
	BackupImage  string
	IPAddress    string
	Username     string
	Password     string
	Volume       types.Volume
	Calico       string // Calico token to enable network access
	ServiceID    string
	DeploymentID string
	Network      string
	Variant      string // mysql, mariadb, bitnami-mariadb
	DataPath     string // Where the data is located in the container
	MountPath    string // Where the volume is mounted. Used to locate the correct volume.
	User         string // The Linux user who should own the data
	Group        string // Linux group who should own th data
}

func loadMysqlMaster(cli *client.Client, serviceID string, event *csevent.ProjectEvent, allowOff bool) (*MysqlInstance, error) {
	ctx := context.Background()
	mysqlFoundContainer, err := containermgr.FindByService(cli, serviceID, allowOff)

	// Versions
	fivesix := decimal.NewFromFloatWithExponent(5.6, -1)
	eight := decimal.NewFromFloatWithExponent(8.0, -1)
	tenone := decimal.NewFromFloatWithExponent(10.1, -1)
	tentwo := decimal.NewFromFloatWithExponent(10.2, -1)
	tenthree := decimal.NewFromFloatWithExponent(10.3, -1)
	tenfour := decimal.NewFromFloatWithExponent(10.4, -1)
	tenfive := decimal.NewFromFloatWithExponent(10.5, -1)
	tensix := decimal.NewFromFloatWithExponent(10.6, -1)
	tenseven := decimal.NewFromFloatWithExponent(10.7, -1)
	teneight := decimal.NewFromFloatWithExponent(10.8, -1)
	tennine := decimal.NewFromFloatWithExponent(10.9, -1)
	tenten := decimal.NewFromFloatWithExponent(10.10, -1)

	// Set defaults
	instance := MysqlInstance{
		Container: mysqlFoundContainer,
		DataPath:  "/var/lib/mysql",
		MountPath: "/var/lib/mysql",
		User:      "mysql",
		Group:     "mysql",
	}

	if err != nil {
		backupLogger().Warn("Failed to load MySQL Master", "error", err.Error(), "function", "loadMysqlMaster", "action", "FindRunningContainer", "serviceID", serviceID)
		go event.PostEventUpdate("agent-6e0d0cbb268ac0b1", "Container appears to be offline. Unable to perform"+" "+event.EventLog.Locale)
		event.EventLog.Status = "cancelled"
		return &instance, err
	}

	mysqlContainer, err := cli.ContainerInspect(ctx, mysqlFoundContainer.ID)

	if err != nil {
		backupLogger().Warn("Failed to load MySQL Master", "error", err.Error(), "function", "loadMysqlMaster", "action", "ContainerInspect")
		return &instance, err
	}

	for _, c := range mysqlContainer.Config.Env {
		keys := strings.Split(c, "=")
		switch keys[0] {
		case "MYSQL_ROOT_PASSWORD":
			instance.Username = "root"
			instance.Password = keys[1]
		case "MYSQL_MAJOR":
			f, fErr := decimal.NewFromString(keys[1])
			if fErr != nil {
				instance.Version = eight // default to v8
			} else {
				instance.Version = f
			}
			instance.Variant = "mysql"
		case "MARIADB_MAJOR":
			//f, fErr := strconv.ParseFloat(keys[1], 64)
			f, fErr := decimal.NewFromString(keys[1])
			if fErr != nil {
				instance.Version = tenfive
			} else {
				instance.Version = f
			}
			instance.Variant = "mariadb"
		case "MARIADB_VERSION":
			// As of 13-Oct-22, MariaDB removed `MARIADB_MAJOR` from v10.9+.
			// Temporarily fallback to 10.9.
			if instance.Variant != "mariadb" { // check for mysql so that if we already parsed _MAJOR, we don't reset it.
				instance.Version = tennine
				instance.Variant = "mariadb"
			}
		case "MYSQL_USER":
			instance.Username = keys[1]
		case "MYSQL_PASSWORD":
			instance.Password = keys[1]
		case "BITNAMI_APP_NAME":
			if keys[1] == "mariadb" {
				instance.Variant = "bitnami-mariadb"
				instance.DataPath = "/bitnami/mariadb/data"
				instance.MountPath = "/bitnami/mariadb"
				instance.User = "1001"
				instance.Group = "root"
			}
		case "BITNAMI_IMAGE_VERSION":
			bitnamiVersionStr := strings.Split(keys[1], ".")
			bitnamiVersionStrCombined := bitnamiVersionStr[0] + "." + bitnamiVersionStr[1]
			bitnamiVersion, bitnamiVersionErr := decimal.NewFromString(bitnamiVersionStrCombined)
			if bitnamiVersionErr != nil {
				backupLogger().Warn("Error parsing bitnami mariadb version", "error", bitnamiVersionErr.Error())
				sentry.CaptureException(bitnamiVersionErr)
				instance.Version = tenfive // Fall back to 10.5
			} else {
				if bitnamiVersion.LessThan(tenone) {
					instance.Version = tenone
				} else if bitnamiVersion.GreaterThan(tensix) {
					instance.Version = tensix
				} else {
					instance.Version = bitnamiVersion
				}
			}
		case "MARIADB_ROOT_PASSWORD": // Bitnami
			instance.Username = "root"
			instance.Password = keys[1]
		}
	}

	for _, v := range mysqlContainer.Mounts {
		if v.Destination == instance.MountPath {
			instance.Volume = types.Volume{Name: v.Name}
		}
	}

	var ipAddr string

	for name, net := range mysqlContainer.NetworkSettings.Networks {
		if net.IPAddress != "" {
			ipAddr = net.IPAddress
			instance.Network = name
			break
		}
	}

	if ipAddr == "" && !allowOff {
		backupLogger().Warn("Failed to load mySQLMaster IP Address", "serviceID", serviceID)
		return &instance, errors.New("unable to load ip address of container")
	} else if ipAddr != "" {
		instance.IPAddress = ipAddr
	}

	// Ensure we load up all our service IDs to allow our backup container full access to the project.
	for k, v := range mysqlContainer.Config.Labels {
		//keys := strings.Split(l, "=")
		backupLogger().Debug("Have MySQL Label", "key", k, "label", v)
		switch k {
		case "org.projectcalico.label.token":
			instance.Calico = v
		case "com.computestacks.deployment_id":
			instance.DeploymentID = v
		case "com.computestacks.service_id":
			instance.ServiceID = v
		}
	}

	// Set Image
	if instance.Variant == "mysql" {
		if instance.Version.GreaterThan(fivesix) {
			instance.BackupImage = "cmptstks/xtrabackup:8.0"
		} else {
			instance.BackupImage = "cmptstks/xtrabackup:2.4"
		}
	} else { // both mariadb and bitnami-mariadb
		switch v := instance.Version; {
		case v.Equal(tenone):
			instance.BackupImage = "cmptstks/mariadb-backup:10.1"
		case v.Equal(tentwo):
			instance.BackupImage = "cmptstks/mariadb-backup:10.2"
		case v.Equal(tenthree):
			instance.BackupImage = "cmptstks/mariadb-backup:10.3"
		case v.Equal(tenfour):
			instance.BackupImage = "cmptstks/mariadb-backup:10.4"
		case v.Equal(tenfive):
			instance.BackupImage = "cmptstks/mariadb-backup:10.5"
		case v.Equal(tensix):
			instance.BackupImage = "cmptstks/mariadb-backup:10.6"
		case v.Equal(tenseven):
			instance.BackupImage = "cmptstks/mariadb-backup:10.7"
		case v.Equal(teneight):
			instance.BackupImage = "cmptstks/mariadb-backup:10.8"
		case v.Equal(tennine):
			instance.BackupImage = "cmptstks/mariadb-backup:10.9"
		case v.Equal(tenten):
			// TODO: Create 10.10 image when it moves to stable.
			instance.BackupImage = "cmptstks/mariadb-backup:10.9"
		default:
			instance.BackupImage = "cmptstks/mariadb-backup:10.9"
		}
	}

	backupLogger().Info("Have MySQL Target", "variant", instance.Variant, "version", instance.Version, "image", instance.BackupImage)

	return &instance, nil
}

func buildBackupAgent(cli *client.Client, mysqlMaster *MysqlInstance) (*containermgr.Container, error) {

	ctx := context.Background()

	backupLogger().Debug("Building backup container for SQL server", "variant", mysqlMaster.Variant, "version", mysqlMaster.Version, "image", mysqlMaster.BackupImage)

	// Ensure image exists
	_, _, missingImage := cli.ImageInspectWithRaw(ctx, mysqlMaster.BackupImage)

	if missingImage != nil {
		_, err := cli.ImagePull(ctx, mysqlMaster.BackupImage, dockerTypes.ImagePullOptions{})
		if err != nil {
			return nil, err
		}
		// Looks like it's moving on too quickly, wait 1 second to make sure image is available.
		time.Sleep(time.Second)
	}

	labels := make(map[string]string)
	if mysqlMaster.Calico != "" {
		labels["org.projectcalico.label.token"] = mysqlMaster.Calico
	}
	if mysqlMaster.DeploymentID != "" {
		labels["com.computestacks.deployment_id"] = mysqlMaster.DeploymentID
	}
	if mysqlMaster.ServiceID != "" {
		labels["com.computestacks.service_id"] = mysqlMaster.ServiceID
	}
	labels["com.computestacks.role"] = "backup"

	backupLogger().Debug("Master Params", "calico", mysqlMaster.Calico, "deployment", mysqlMaster.DeploymentID, "service", mysqlMaster.ServiceID)

	t := time.Now()

	if mysqlMaster.Volume.IsEmpty() {
		backupLogger().Debug("MySQL Master volume is nil")
		return nil, errors.New("database master has no volume to mount")
	}
	backupLogger().Debug("Have MySQL Volume", "volume", mysqlMaster.Volume.Name)

	env := []string{"MYSQL_HOST=" + mysqlMaster.IPAddress, "MYSQL_PASSWORD=" + mysqlMaster.Password} // Used by init script to ensure MySQL is available before attempting a backup

	rand.Seed(time.Now().UnixNano()) // Seed for random container name
	randNumber := 10 + rand.Intn(1000-10)
	containerName := "backup-" + strconv.Itoa(randNumber) + string(t.Format("150405"))

	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image:  mysqlMaster.BackupImage,
		Labels: labels,
		Env:    env,
	}, &container.HostConfig{
		NetworkMode: container.NetworkMode(mysqlMaster.Network),
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeVolume,
				Source: mysqlMaster.Volume.Name,
				Target: mysqlMaster.MountPath,
			},
		},
		AutoRemove: true,
	}, nil, containerName)
	if err != nil {
		return nil, err
	}

	if err := cli.ContainerStart(ctx, resp.ID, dockerTypes.ContainerStartOptions{}); err != nil {
		return nil, err
	}

	time.Sleep(2 * time.Second)

	return &containermgr.Container{ID: resp.ID}, nil
}
