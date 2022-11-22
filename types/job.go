package types

import (
	"cs-agent/log"
	"encoding/json"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
)

type Job struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	VolumeName       string   `json:"volume"`
	SourceVolumeName string   `json:"source_volume"` // Used to restore backup from a different volume
	ArchiveName      string   `json:"archive"`       // Used for creating,restoring
	FilePaths        []string `json:"file_paths"`    // Used for restoring (DEPRECATED)
	AuditID          int      `json:"audit_id"`
	Node             string   `json:"node"` // The requested node's hostname
}

func (job *Job) JSONEncode() []byte {
	jsonData, _ := json.Marshal(job)
	return jsonData
}

func (job *Job) Save(consul *consulAPI.Client) error {
	kv := consul.KV()
	kp := consulAPI.KVPair{
		Key:   job.ID, // jobs/jobID
		Value: job.JSONEncode(),
	}
	_, err := kv.Put(&kp, nil)
	return err
}

func (job *Job) Close(consul *consulAPI.Client) bool {
	//log.New().Named("worker").Info("Finished job", "job", job.ID)
	kv := consul.KV()
	_, err := kv.Delete(job.ID, nil)
	if err != nil {
		log.New().Named("worker").Warn("Fatal error while cleaning up job", "jobID", job.ID, "error", err.Error())
		sentry.CaptureException(err)
	}
	return true
}
