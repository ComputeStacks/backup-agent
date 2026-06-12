package borg

import (
	"testing"

	"github.com/spf13/viper"
)

func TestSafeRepoName(t *testing.T) {
	cases := map[string]bool{
		"12345-12345-12345-1234567": true,
		"my_volume.name-1":          true,
		"a":                         true,
		"":                          false,
		"-leading-dash":             false,
		"has space":                 false,
		"semi;rm -rf /":             false,
		"$(whoami)":                 false,
		"tick`x`":                   false,
		"slash/name":                false,
	}
	for name, want := range cases {
		if got := safeRepoName(name); got != want {
			t.Errorf("safeRepoName(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestNFSCompactCommand(t *testing.T) {
	viper.Reset()
	viper.Set("backups.borg.nfs_host_path", "/mnt/ams001/node001")
	viper.Set("backups.borg.nfs_borg_path", "/usr/local/bin/borg")
	viper.Set("backups.borg.nfs_ssh.fs_user", "nobody")
	viper.Set("backups.borg.nfs_ssh.fs_group", "nogroup")

	cmd, ok := nfsCompactCommand("vol-1")
	if !ok {
		t.Fatal("expected ok for a valid repository name")
	}
	want := "/usr/local/bin/borg compact --verbose --log-json /mnt/ams001/node001/b-vol-1/backup" +
		" && chown -R nobody:nogroup /mnt/ams001/node001/b-vol-1"
	if cmd != want {
		t.Errorf("nfsCompactCommand mismatch:\n got: %s\nwant: %s", cmd, want)
	}

	if _, ok := nfsCompactCommand("bad;name"); ok {
		t.Error("expected an unsafe repository name to be rejected")
	}
}
