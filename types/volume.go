package types

import (
	"encoding/json"
	"os"

	consulAPI "github.com/hashicorp/consul/api"
	"github.com/robfig/cron/v3"
)

type Volume struct {
	ID        int    `json:"id"`   // ComputeStacks ID
	Name      string `json:"name"` // Docker Name
	Node      string `json:"node"`
	Backup    bool   `json:"backup"`
	Freq      string `json:"freq"` // Cron syntax
	Retention struct {
		Hourly   int `json:"keep_hourly"`
		Daily    int `json:"keep_daily"`
		Weekly   int `json:"keep_weekly"`
		Monthly  int `json:"keep_monthly"`
		Annually int `json:"keep_annually"`
	} `json:"retention"`
	LastBackup             int64    `json:"last_backup"`
	ProjectID              int      `json:"project_id"`
	ServiceID              int      `json:"service_id"`
	Trash                  bool     `json:"trash"`
	Strategy               string   `json:"strategy"` // file, mysql, postgres
	PreBackup              []string `json:"pre_backup"`
	PostBackup             []string `json:"post_backup"`
	PreRestore             []string `json:"pre_restore"`
	PostRestore            []string `json:"post_restore"`
	RollbackRestore        []string `json:"rollback_restore"`   // Run this when recovering from a restore (after PreRestore runs)
	BackupContinueOnError  bool     `json:"backup_error_cont"`  // Continue if an error is encountered with `pre_backup`
	RestoreContinueOnError bool     `json:"restore_error_cont"` // Continue if an error is encountered with `pre_restore`
}

type VolumeJob struct {
	JID      cron.EntryID `json:"jid"`
	Schedule string       `json:"schedule"`
}

func LoadVolume(value []byte) (Volume, error) {
	var vol Volume
	err := json.Unmarshal(value, &vol)
	if err != nil {
		return Volume{}, err
	}
	return vol, nil
}

func (vol Volume) JSONEncode() []byte {
	if j, err := json.Marshal(vol); err == nil {
		return j
	}
	return []byte{}
}

// Find the active job for this volume
func (vol Volume) ScheduledJob(consul *consulAPI.Client) (*VolumeJob, error) {
	var vj VolumeJob
	hostname, _ := os.Hostname()
	kv := consul.KV()
	volumePath := "borg/nodes/" + hostname + "/schedules/" + vol.Name
	data, _, err := kv.Get(volumePath, nil)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	if jsonErr := json.Unmarshal(data.Value, &vj); jsonErr != nil {
		return nil, jsonErr
	}

	return &vj, nil
}

func (vol Volume) ClearScheduledJob(consul *consulAPI.Client) {
	hostname, _ := os.Hostname()
	volumePath := "borg/nodes/" + hostname + "/schedules/" + vol.Name
	kv := consul.KV()
	_, _ = kv.Delete(volumePath, nil)
}

func (vol Volume) AddScheduledJob(consul *consulAPI.Client, jid cron.EntryID) error {
	volJob := VolumeJob{
		JID:      jid,
		Schedule: vol.Freq,
	}
	hostname, _ := os.Hostname()
	kv := consul.KV()
	volumePath := "borg/nodes/" + hostname + "/schedules/" + vol.Name
	//jIdString := strconv.FormatInt(int64(volJob.JID), 10)

	data, err := json.Marshal(volJob)

	if err != nil {
		return err
	}

	kp := consulAPI.KVPair{
		Key:   volumePath,
		Value: data,
	}
	if _, consulErr := kv.Put(&kp, nil); consulErr != nil {
		return consulErr
	}
	return nil
}

func (vol Volume) IsEmpty() bool {
	return vol.Name == ""
}
