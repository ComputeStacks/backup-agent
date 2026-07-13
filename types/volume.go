package types

import (
	"encoding/json"
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

func (vol Volume) IsEmpty() bool {
	return vol.Name == ""
}
