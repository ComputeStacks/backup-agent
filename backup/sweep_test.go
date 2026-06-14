package backup

import (
	"cs-agent/backup/borg"
	"cs-agent/config"
	"cs-agent/types"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	consulAPI "github.com/hashicorp/consul/api"
	"github.com/spf13/viper"
)

// keysErrConsulKV is a MockConsulKV whose Keys() fails, to exercise the
// sweep's list-error path.
type keysErrConsulKV struct{ *MockConsulKV }

func (k keysErrConsulKV) Keys(prefix, sep string, q *consulAPI.QueryOptions) ([]string, *consulAPI.QueryMeta, error) {
	return nil, nil, errors.New("consul unavailable")
}

func putVolume(m *MockConsulKV, name, node string) {
	m.store["volumes/"+name] = types.Volume{Name: name, Node: node}.JSONEncode()
}

func putRecord(t *testing.T, m *MockConsulKV, key string, d borg.ConsulDownload) {
	t.Helper()
	j, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal record for %s: %v", key, err)
	}
	m.store[key] = j
}

func assertAbsent(t *testing.T, m *MockConsulKV, key string) {
	t.Helper()
	if _, ok := m.store[key]; ok {
		t.Errorf("expected %q to be reaped, but it is still present", key)
	}
}

func assertPresent(t *testing.T, m *MockConsulKV, key string) {
	t.Helper()
	if _, ok := m.store[key]; !ok {
		t.Errorf("expected %q to be kept, but it was deleted", key)
	}
}

func TestSweepExports(t *testing.T) {
	viper.Reset()
	config.ConfigureApp() // failed_retention_sec = 86400
	host, _ := os.Hostname()
	now := time.Now().Unix()
	m := &MockConsulKV{store: make(map[string][]byte)}

	// Volumes: one owned by this node, one owned by another, one reassigned to
	// this node, and "missingvol" which has no volumes/ entry at all.
	putVolume(m, "owned", host)
	putVolume(m, "foreign", "other-node")
	putVolume(m, "reassigned", host)

	// --- records to REAP ---
	putRecord(t, m, borg.DownloadKey("owned", "c-exp"),
		borg.ConsulDownload{Status: borg.DownloadStatusCompleted, URL: "https://x", Expiry: now - 100, UpdatedAt: now - 200}) // 1
	putRecord(t, m, borg.DownloadKey("owned", "f-old"),
		borg.ConsulDownload{Status: borg.DownloadStatusFailed, Error: "boom", UpdatedAt: now - 90000}) // 4 (> 86400)
	putRecord(t, m, borg.DownloadKey("reassigned", "c-exp"),
		borg.ConsulDownload{Status: borg.DownloadStatusCompleted, URL: "https://y", Expiry: now - 100, UpdatedAt: now - 200}) // 12a

	// --- records to KEEP ---
	putRecord(t, m, borg.DownloadKey("owned", "c-fresh"),
		borg.ConsulDownload{Status: borg.DownloadStatusCompleted, URL: "https://x", Expiry: now + 3600, UpdatedAt: now}) // 2
	putRecord(t, m, borg.DownloadKey("owned", "c-noexp"),
		borg.ConsulDownload{Status: borg.DownloadStatusCompleted, URL: "https://x", Expiry: 0, UpdatedAt: now - 99999}) // 3
	putRecord(t, m, borg.DownloadKey("owned", "f-recent"),
		borg.ConsulDownload{Status: borg.DownloadStatusFailed, Error: "boom", UpdatedAt: now - 100}) // 5
	putRecord(t, m, borg.DownloadKey("owned", "f-noupd"),
		borg.ConsulDownload{Status: borg.DownloadStatusFailed, Error: "boom", UpdatedAt: 0}) // 6
	putRecord(t, m, borg.DownloadKey("owned", "running"),
		borg.ConsulDownload{Status: borg.DownloadStatusRunning, UpdatedAt: now - 999999}) // 7 (any age kept)
	putRecord(t, m, borg.DownloadKey("foreign", "c-exp"),
		borg.ConsulDownload{Status: borg.DownloadStatusCompleted, URL: "https://x", Expiry: now - 100, UpdatedAt: now - 200}) // 8
	putRecord(t, m, borg.DownloadKey("missingvol", "c-exp"),
		borg.ConsulDownload{Status: borg.DownloadStatusCompleted, URL: "https://x", Expiry: now - 100, UpdatedAt: now - 200}) // 9
	m.store["borg/exports/onlythree"] = []byte(`{"status":"completed"}`) // 10 malformed key
	m.store[borg.DownloadKey("owned", "bad")] = []byte(`{not json`)      // 11 unparseable

	sweepExports(m)

	for _, k := range []string{
		borg.DownloadKey("owned", "c-exp"),
		borg.DownloadKey("owned", "f-old"),
		borg.DownloadKey("reassigned", "c-exp"),
	} {
		assertAbsent(t, m, k)
	}
	for _, k := range []string{
		borg.DownloadKey("owned", "c-fresh"),
		borg.DownloadKey("owned", "c-noexp"),
		borg.DownloadKey("owned", "f-recent"),
		borg.DownloadKey("owned", "f-noupd"),
		borg.DownloadKey("owned", "running"),
		borg.DownloadKey("foreign", "c-exp"),
		borg.DownloadKey("missingvol", "c-exp"),
		"borg/exports/onlythree",
		borg.DownloadKey("owned", "bad"),
	} {
		assertPresent(t, m, k)
	}
}

func TestSweepExportsEmpty(t *testing.T) {
	viper.Reset()
	config.ConfigureApp()
	m := &MockConsulKV{store: make(map[string][]byte)}
	sweepExports(m) // must not panic
	if len(m.store) != 0 {
		t.Errorf("expected empty store, got %d keys", len(m.store))
	}
}

func TestSweepExportsKeysError(t *testing.T) {
	viper.Reset()
	config.ConfigureApp()
	host, _ := os.Hostname()
	m := &MockConsulKV{store: make(map[string][]byte)}
	putVolume(m, "owned", host)
	key := borg.DownloadKey("owned", "c-exp")
	putRecord(t, m, key, borg.ConsulDownload{Status: borg.DownloadStatusCompleted, Expiry: time.Now().Unix() - 100})

	sweepExports(keysErrConsulKV{m}) // must log+return, not panic

	assertPresent(t, m, key) // nothing reaped when the listing failed
}

func TestDeleteVolumeExportsPrefixCollision(t *testing.T) {
	m := &MockConsulKV{store: make(map[string][]byte)}
	m.store[borg.DownloadKey("vol1", "j1")] = []byte(`{}`)
	m.store[borg.DownloadKey("vol1", "j2")] = []byte(`{}`)
	m.store[borg.DownloadKey("vol10", "j1")] = []byte(`{}`)

	deleteVolumeExports(m, "vol1")

	assertAbsent(t, m, borg.DownloadKey("vol1", "j1"))
	assertAbsent(t, m, borg.DownloadKey("vol1", "j2"))
	assertPresent(t, m, borg.DownloadKey("vol10", "j1")) // trailing slash must not match vol10
}

func TestFinalizeStuckExport(t *testing.T) {
	key := borg.DownloadKey("v", "j")

	t.Run("running flipped to failed", func(t *testing.T) {
		m := &MockConsulKV{store: make(map[string][]byte)}
		putRecord(t, m, key, borg.ConsulDownload{Status: borg.DownloadStatusRunning, UpdatedAt: 100})

		finalizeStuckExport(m, key)

		var d borg.ConsulDownload
		if err := json.Unmarshal(m.store[key], &d); err != nil {
			t.Fatal(err)
		}
		if d.Status != borg.DownloadStatusFailed {
			t.Errorf("expected failed, got %q", d.Status)
		}
		// Fresh struct: no stale url/expiry leaked via omitempty.
		if s := string(m.store[key]); strings.Contains(s, "url") || strings.Contains(s, "expiry") {
			t.Errorf("failed record must not carry url/expiry, got %s", s)
		}
	})

	t.Run("completed untouched (URL/Expiry survive)", func(t *testing.T) {
		m := &MockConsulKV{store: make(map[string][]byte)}
		putRecord(t, m, key, borg.ConsulDownload{Status: borg.DownloadStatusCompleted, URL: "https://x", Expiry: 999, UpdatedAt: 100})

		finalizeStuckExport(m, key)

		var d borg.ConsulDownload
		if err := json.Unmarshal(m.store[key], &d); err != nil {
			t.Fatal(err)
		}
		if d.Status != borg.DownloadStatusCompleted || d.URL != "https://x" || d.Expiry != 999 {
			t.Errorf("completed record must be untouched, got %+v", d)
		}
	})

	t.Run("failed untouched", func(t *testing.T) {
		m := &MockConsulKV{store: make(map[string][]byte)}
		putRecord(t, m, key, borg.ConsulDownload{Status: borg.DownloadStatusFailed, Error: "orig", UpdatedAt: 100})

		finalizeStuckExport(m, key)

		var d borg.ConsulDownload
		if err := json.Unmarshal(m.store[key], &d); err != nil {
			t.Fatal(err)
		}
		if d.Status != borg.DownloadStatusFailed || d.Error != "orig" {
			t.Errorf("failed record must be untouched, got %+v", d)
		}
	})

	t.Run("missing record no-op", func(t *testing.T) {
		m := &MockConsulKV{store: make(map[string][]byte)}
		finalizeStuckExport(m, key) // must not panic
		if len(m.store) != 0 {
			t.Errorf("expected no record written, got %d", len(m.store))
		}
	})
}
