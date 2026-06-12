package borg

import (
	"cs-agent/cnslclient"
	"encoding/json"
	"strings"

	consulAPI "github.com/hashicorp/consul/api"
)

// Status values for a ConsulDownload record.
const (
	DownloadStatusRunning   = "running"
	DownloadStatusCompleted = "completed"
	DownloadStatusFailed    = "failed"
)

// ConsulDownload is the result record for a backup export, written by the agent
// to borg/exports/<volume>/<jid> and read on-demand by ComputeStacks. The volume
// and jid are implied by the key, so they are not duplicated in the body.
type ConsulDownload struct {
	Status    string `json:"status"`               // running | completed | failed
	URL       string `json:"url,omitempty"`        // presigned GET URL (completed)
	ObjectKey string `json:"object_key,omitempty"` // full S3 object key (completed)
	Size      int64  `json:"size,omitempty"`       // uploaded bytes (completed)
	Expiry    int64  `json:"expiry,omitempty"`     // unix time the URL expires (completed)
	Error     string `json:"error,omitempty"`      // failure reason (failed)
	UpdatedAt int64  `json:"updated_at"`
}

// DownloadKey is the Consul KV key for a volume's export job.
func DownloadKey(volume, jid string) string {
	return "borg/exports/" + volume + "/" + jid
}

// DownloadVolumeFromKey extracts the volume name from a borg/exports/<volume>/<jid>
// key, or "" if the key isn't in that shape. (Volume names and jids never contain
// "/", so a well-formed key has exactly four segments.)
func DownloadVolumeFromKey(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0] != "borg" || parts[1] != "exports" {
		return ""
	}
	return parts[2]
}

// Save writes the record to the given full KV key.
func (d *ConsulDownload) Save(key string) error {
	consul, err := cnslclient.Client()
	if err != nil {
		return err
	}
	j, err := json.Marshal(d)
	if err != nil {
		return err
	}
	_, err = consul.KV().Put(&consulAPI.KVPair{Key: key, Value: j}, nil)
	return err
}

// ListDownloads returns every export record keyed by its full KV key.
func ListDownloads() (map[string]*ConsulDownload, error) {
	consul, err := cnslclient.Client()
	if err != nil {
		return nil, err
	}
	pairs, _, err := consul.KV().List("borg/exports/", nil)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*ConsulDownload, len(pairs))
	for _, p := range pairs {
		var d ConsulDownload
		if json.Unmarshal(p.Value, &d) == nil {
			out[p.Key] = &d
		}
	}
	return out, nil
}
