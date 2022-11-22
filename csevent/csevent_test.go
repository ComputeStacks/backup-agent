package csevent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/viper"
)

func TestCreateEvent(t *testing.T) {
	httpEventItem := EventItem{
		ID:            1,
		Locale:        "volume.backup",
		DeploymentIds: []int{1},
		VolumeIds:     []int{4, 5, 6},
		Status:        "running",
		EventCode:     "agent-51df3bbaed3ce2c4",
		EventDetails:  make([]ProjectDetail, 0)}

	httpEvent := ProjectEvent{httpEventItem}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := json.Marshal(httpEvent)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.String() == "/api/system/events" {
			_, _ = w.Write(data)
		}

	}))
	defer server.Close()
	viper.Set("computestacks.host", server.URL)

	newEvent := New(httpEventItem.ID, httpEventItem.VolumeIds, "agent-51df3bbaed3ce2c4", "volume.backup", 0)

	t.Logf("%s", viper.GetString("backups.key"))

	if newEvent.EventLog.ID != httpEventItem.ID {
		t.Error("New project ID does not matched expected ID.")
	}

}
