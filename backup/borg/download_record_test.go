package borg

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDownloadKeyRoundTrip(t *testing.T) {
	key := DownloadKey("vol-1", "jid-abc")
	if key != "borg/exports/vol-1/jid-abc" {
		t.Errorf("DownloadKey = %q", key)
	}
	if got := DownloadVolumeFromKey(key); got != "vol-1" {
		t.Errorf("DownloadVolumeFromKey(%q) = %q, want vol-1", key, got)
	}
	if got := DownloadJidFromKey(key); got != "jid-abc" {
		t.Errorf("DownloadJidFromKey(%q) = %q, want jid-abc", key, got)
	}
	for _, bad := range []string{
		"",
		"borg/exports/vol-1",     // too few segments
		"borg/exports/v/j/extra", // too many
		"other/exports/v/j",      // wrong root
		"borg/repository/v/j",    // wrong namespace
	} {
		if got := DownloadVolumeFromKey(bad); got != "" {
			t.Errorf("DownloadVolumeFromKey(%q) = %q, want empty", bad, got)
		}
		if got := DownloadJidFromKey(bad); got != "" {
			t.Errorf("DownloadJidFromKey(%q) = %q, want empty", bad, got)
		}
	}
}

func TestConsulDownloadMarshal(t *testing.T) {
	running, _ := json.Marshal(ConsulDownload{Status: DownloadStatusRunning, UpdatedAt: 100})
	for _, f := range []string{"url", "object_key", "size", "expiry", "error"} {
		if strings.Contains(string(running), f) {
			t.Errorf("a running record should omit %q, got %s", f, running)
		}
	}
	completed, _ := json.Marshal(ConsulDownload{Status: DownloadStatusCompleted, URL: "https://x", Size: 5, Expiry: 200, UpdatedAt: 100})
	if !strings.Contains(string(completed), `"url":"https://x"`) {
		t.Errorf("a completed record must include the url, got %s", completed)
	}
}
