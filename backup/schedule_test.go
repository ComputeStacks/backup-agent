package backup

import (
	"cs-agent/config"
	"cs-agent/types"
	"testing"

	consulAPI "github.com/hashicorp/consul/api"

	"github.com/robfig/cron/v3"
	"github.com/spf13/viper"
)

func TestInitSchedule(t *testing.T) {

	viper.Reset()
	config.ConfigureApp()
	c := cron.New()
	mockConsul := &MockConsulKV{
		store: make(map[string][]byte),
	}
	InitSchedule(mockConsul, c)

	fakeVolume := types.Volume{
		Name: "12345-12345-12345-1234567",
		Freq: "*/10 * * * *",
	}

	//defer fakeVolume.ClearScheduledJob(consul)

	if len(c.Entries()) != 3 {
		t.Errorf("Expected 3 jobs to be created (check, prune, compact), but got %d", len(c.Entries()))
	} else {
		t.Logf("Successfully found %d jobs", len(c.Entries()))
	}

	jid, err := c.AddFunc("* * * * *", func() { t.Log("job ran") })

	if err != nil {
		t.Errorf("Failed to schedule job: %s", err.Error())
		return
	}

	if len(c.Entries()) != 4 {
		t.Errorf("I expected 4 jobs, but instead i got %d", len(c.Entries()))
		return
	}

	t.Logf("Successfully scheduled test job: %d", jid)

	// Record scheduled job
	scheduleJobErr := fakeVolume.AddScheduledJob(mockConsul, jid)

	if scheduleJobErr != nil {
		t.Errorf("Failed to record scheduled job: %s", scheduleJobErr.Error())
	}

	// Load Scheduled Job
	fakeVolumeJob, fakeVolumeJobErr := fakeVolume.ScheduledJob(mockConsul)

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

	if len(c.Entries()) != 3 {
		t.Errorf("I expected 3 jobs, but instead i got %d", len(c.Entries()))
		return
	}

	t.Logf("Successfully deleted job %d, now I only have %d jobs.", jid, len(c.Entries()))

	// TODO: Create fake volume and try to push it to the job queue to test how the cron system would do it.

}

// An empty backups.compact_freq must disable compaction scheduling (only the
// check and prune entries remain). Guards against an empty-default regression
// that a count-only assertion in TestInitSchedule wouldn't otherwise catch.
func TestInitScheduleCompactDisabled(t *testing.T) {
	viper.Reset()
	config.ConfigureApp()
	viper.Set("backups.compact_freq", "")

	c := cron.New()
	mockConsul := &MockConsulKV{store: make(map[string][]byte)}
	InitSchedule(mockConsul, c)

	if len(c.Entries()) != 2 {
		t.Errorf("with compact_freq empty, expected 2 cron entries (check, prune), got %d", len(c.Entries()))
	}
}

// With an export bucket configured and the default cleanup_freq, the sweep
// entry registers alongside check/prune/compact (4 total).
func TestInitScheduleExportCleanupEnabled(t *testing.T) {
	viper.Reset()
	config.ConfigureApp()
	viper.Set("backups.export.s3.bucket", "team-exports")

	c := cron.New()
	mockConsul := &MockConsulKV{store: make(map[string][]byte)}
	InitSchedule(mockConsul, c)

	if len(c.Entries()) != 4 {
		t.Errorf("with export bucket + cleanup_freq set, expected 4 cron entries (check, prune, compact, sweep), got %d", len(c.Entries()))
	}
}

// A bucket but an empty cleanup_freq must NOT register the sweep entry.
func TestInitScheduleExportCleanupDisabled(t *testing.T) {
	viper.Reset()
	config.ConfigureApp()
	viper.Set("backups.export.s3.bucket", "team-exports")
	viper.Set("backups.export.cleanup_freq", "")

	c := cron.New()
	mockConsul := &MockConsulKV{store: make(map[string][]byte)}
	InitSchedule(mockConsul, c)

	if len(c.Entries()) != 3 {
		t.Errorf("with cleanup_freq empty, expected 3 cron entries (check, prune, compact), got %d", len(c.Entries()))
	}
}

type MockConsulKV struct {
	store map[string][]byte
}

func (m *MockConsulKV) Get(key string, q *consulAPI.QueryOptions) (*consulAPI.KVPair, *consulAPI.QueryMeta, error) {
	val, ok := m.store[key]
	if !ok {
		return nil, nil, nil
	}
	return &consulAPI.KVPair{Key: key, Value: val}, nil, nil
}

func (m *MockConsulKV) Put(p *consulAPI.KVPair, q *consulAPI.WriteOptions) (*consulAPI.WriteMeta, error) {
	m.store[p.Key] = p.Value
	return nil, nil
}

func (m *MockConsulKV) Delete(key string, w *consulAPI.WriteOptions) (*consulAPI.WriteMeta, error) {
	delete(m.store, key)
	return nil, nil
}

func (m *MockConsulKV) DeleteTree(prefix string, w *consulAPI.WriteOptions) (*consulAPI.WriteMeta, error) {
	for k := range m.store {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(m.store, k)
		}
	}
	return nil, nil
}

func (m *MockConsulKV) Keys(prefix, separator string, q *consulAPI.QueryOptions) ([]string, *consulAPI.QueryMeta, error) {
	var keys []string
	for k := range m.store {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil, nil
}
