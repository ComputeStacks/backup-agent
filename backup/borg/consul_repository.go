package borg

import (
	"cs-agent/cnslclient"
	"encoding/json"

	"github.com/getsentry/sentry-go"
	consulAPI "github.com/hashicorp/consul/api"
)

type ConsulRepository struct {
	Name       string   `json:"name"`               // Name, should match the volume name.
	SizeOnDisk int      `json:"usage,omitempty"`    // Actual on-disk usage
	TotalSize  int      `json:"size,omitempty"`     // Total, non-deduped, size
	Archives   []string `json:"archives,omitempty"` // List of archive names
}

func (r *ConsulRepository) Save() error {

	consul, err := cnslclient.Client()
	if err != nil {
		return err
	}
	kvClient := consul.KV()
	kp := consulAPI.KVPair{
		Key:   "borg/repository/" + r.Name,
		Value: r.JSONEncode(),
	}
	_, consulErr := kvClient.Put(&kp, nil)

	return consulErr

}

// Formatting to JSON
func (r *ConsulRepository) JSONEncode() []byte {
	j, _ := json.Marshal(r)
	return j
}

func (r *ConsulRepository) Delete() error {
	consul, err := cnslclient.Client()
	if err != nil {
		return err
	}
	kvClient := consul.KV()

	_, err = kvClient.Delete("borg/repository/"+r.Name, nil)

	if err != nil {
		return err
	}
	return nil
}

func FindConsulRepo(name string) (*ConsulRepository, error) {
	var cr ConsulRepository
	consul, err := cnslclient.Client()
	if err != nil {
		return &cr, err
	}
	kvClient := consul.KV()

	// kp.Value = {"name": "repo-name"}
	kp, _, err := kvClient.Get("borg/repository/"+name, nil)

	if err != nil {
		return nil, err
	}

	// Create repo in Consul
	if kp == nil {
		cr.Name = name
		createErr := cr.Save()

		if createErr != nil {
			return nil, createErr
		}
		return &cr, nil
	}

	jsonError := json.Unmarshal(kp.Value, &cr)

	if jsonError != nil {
		borgLogger().Warn("Found repo in consul, but failed to unmarshal", "repo", name)
		sentry.CaptureException(jsonError)
		return nil, jsonError
	}

	return &cr, nil

}
