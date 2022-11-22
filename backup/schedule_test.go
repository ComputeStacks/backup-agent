package backup

import (
	"cs-agent/cnslclient"
	"cs-agent/types"
	"testing"

	"github.com/robfig/cron/v3"
)

func TestInitSchedule(t *testing.T) {

	c := cron.New()
	InitSchedule(c)

	consul, err := cnslclient.Client()
	if err != nil {
		t.Errorf("Error connecting to consul: %s", err.Error())
		return
	}

	fakeVolume := types.Volume{
		Name: "12345-12345-12345-1234567",
		Freq: "*/10 * * * *",
	}

	defer fakeVolume.ClearScheduledJob(consul)

	if len(c.Entries()) != 2 {
		t.Errorf("Expected 2 jobs to be created, but got %d", len(c.Entries()))
	} else {
		t.Logf("Successfully found %d jobs", len(c.Entries()))
	}

	jid, err := c.AddFunc("* * * * *", func() { t.Log("job ran") })

	if err != nil {
		t.Errorf("Failed to schedule job: %s", err.Error())
		return
	}

	if len(c.Entries()) != 3 {
		t.Errorf("I expected 3 jobs, but instead i got %d", len(c.Entries()))
		return
	}

	t.Logf("Successfully scheduled test job: %d", jid)

	// Record scheduled job
	scheduleJobErr := fakeVolume.AddScheduledJob(consul, jid)

	if scheduleJobErr != nil {
		t.Errorf("Failed to record scheduled job: %s", scheduleJobErr.Error())
	}

	// Load Scheduled Job
	fakeVolumeJob, fakeVolumeJobErr := fakeVolume.ScheduledJob(consul)

	if fakeVolumeJobErr != nil {
		t.Errorf("Failed to load newly created scheduled job: %s", fakeVolumeJobErr.Error())
	} else {

		// JID
		if fakeVolumeJob.JID != jid {
			t.Error("Stored JID does not match expected jID")
		} else {
			t.Log("JID stored in consul matches expected JID")
		}

		// Schedule
		if fakeVolumeJob.Schedule != fakeVolume.Freq {
			t.Errorf("Expected schedule to be '%s', but got '%s'", fakeVolume.Freq, fakeVolumeJob.Schedule)
		} else {
			t.Log("Pulled correct schedule from consul for volume")
		}
	}

	c.Remove(jid)

	if len(c.Entries()) != 2 {
		t.Errorf("I expected 2 jobs, but instead i got %d", len(c.Entries()))
		return
	}

	t.Logf("Successfully deleted job %d, now I only have %d jobs.", jid, len(c.Entries()))

	// TODO: Create fake volume and try to push it to the job queue to test how the cron system would do it.

}
