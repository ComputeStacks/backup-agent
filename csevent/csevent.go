package csevent

import (
	"cs-agent/log"
	"encoding/json"
	"io/ioutil"
	"reflect"
	"strconv"

	"github.com/getsentry/sentry-go"
	"github.com/hashicorp/go-hclog"
)

// EventItem struct
type EventItem struct {
	ID         int    `json:"id"`
	Locale     string `json:"locale"`
	LocaleKeys struct {
		Volumes int `json:"volumes"`
	} `json:"locale_keys"`
	DeploymentIds []int           `json:"deployment_ids,omitempty"`
	VolumeIds     []int           `json:"volume_ids"`
	Status        string          `json:"status"`
	EventCode     string          `json:"event_code"`
	EventDetails  []ProjectDetail `json:"event_details_attributes,omitempty"`
	AuditID       int             `json:"audit_id,omitempty"`
}

// ProjectDetail struct
type ProjectDetail struct {
	Data      string `json:"data"`
	EventCode string `json:"event_code"`
}

// ProjectEvent struct
type ProjectEvent struct {
	EventLog EventItem `json:"event_log"`
}

// New creates a new ComputeStack event
func New(projectID int, volumeIds []int, eventCode string, locale string, audit int) *ProjectEvent {
	defer sentry.Recover()
	var projectEvent ProjectEvent
	eventItem := EventItem{
		ID:           0,
		Locale:       locale,
		VolumeIds:    volumeIds,
		Status:       "running",
		EventCode:    eventCode,
		EventDetails: make([]ProjectDetail, 0)}
	if audit != 0 {
		eventItem.AuditID = audit
	}
	if projectID > 0 {
		eventItem.DeploymentIds = []int{projectID}
	}
	event := ProjectEvent{eventItem}
	jsonData, _ := json.Marshal(event)
	response, err := post("/api/system/events", jsonData)
	/**
	It's more important to perform the backup then to create an event log
	in ComputeStacks. For most errors (except JSON unmarshal errors), just return
	a `ProjectEvent` with an ID of 0.
	*/
	if err == nil {
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			rData, _ := ioutil.ReadAll(response.Body)
			jsonErr := json.Unmarshal(rData, &projectEvent)
			if jsonErr != nil {
				sentry.CaptureException(jsonErr)
				csEventLog().Error("Error parsing response as json", "data", string(rData))
				return nil
			}
		} else {
			csEventLog().Error("Error received from ComputeStacks", "func", "csevent.New()")
			return nil
		}
	} else {
		sentry.CaptureException(err)
		csEventLog().Error("Error creating projectEvent", "error", err.Error())
		return nil
	}
	return &projectEvent
}

// PostEventUpdate creates a new event on ComputeStacks
func (event *ProjectEvent) PostEventUpdate(code string, y string) {
	defer sentry.Recover()
	if reflect.ValueOf(event).IsNil() {
		return
	}
	if event.EventLog.ID != 0 && y != "" {
		eventID := strconv.Itoa(event.EventLog.ID)
		msg := ProjectDetail{
			Data:      y,
			EventCode: code,
		}
		event.EventLog.EventDetails = []ProjectDetail{msg}
		jsonData, _ := json.Marshal(event)
		_, err := patch("/api/system/events/"+eventID, jsonData)
		if err != nil {
			sentry.CaptureException(err)
			csEventLog().Error("Error patching projectEvent", "error", err.Error())
		}
	}
}

// CloseEvent updates the status to closed
func (event *ProjectEvent) CloseEvent() {
	defer sentry.Recover()
	if reflect.ValueOf(event).IsNil() {
		return
	}
	if event.EventLog.ID != 0 {
		eventID := strconv.Itoa(event.EventLog.ID)
		if event.EventLog.Status == "running" {
			event.EventLog.Status = "completed"
		}
		event.EventLog.EventDetails = make([]ProjectDetail, 0)
		jsonData, _ := json.Marshal(event)
		_, err := patch("/api/system/events/"+eventID, jsonData)
		if err != nil {
			sentry.CaptureException(err)
			csEventLog().Error("Error closing projectEvent", "error", err.Error())
		}
	}

}

func csEventLog() hclog.Logger {
	return log.New().Named("cs-event")
}
