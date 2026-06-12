package borg

import (
	"context"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

// ExportTar streams the archive as a tar to w using `borg export-tar`.
//
// It runs with --bypass-lock: the export is read-only and the repo uses
// authenticated encryption, so a concurrent writer can only make the export
// FAIL (surfaced via the exit code below), never corrupt the repo or a backup.
// The one operation that can break it is `borg compact` (it rewrites segments),
// so callers MUST hold the per-repo lock (AcquireRepoLock) for the whole stream
// to exclude an in-agent compact of the same repo.
//
// w receives raw tar bytes; borg's --log-json diagnostics arrive on stderr and
// are parsed into a LogMessage. A non-nil return means the tar written to w is
// incomplete/untrustworthy and must NOT be published.
func (a *Archive) ExportTar(ctx context.Context, w io.Writer) *LogMessage {
	if a.Repository == nil {
		return &LogMessage{Message: "Missing Repository"}
	}
	if reflect.ValueOf(a.Repository.Container).IsNil() {
		return &LogMessage{Message: "Missing backup container"}
	}

	cmd := []string{"borg --log-json --bypass-lock"}
	cmd = append(cmd, "export-tar")
	if f := viper.GetString("backups.export.tar_filter"); f != "" {
		cmd = append(cmd, "--tar-filter='"+f+"'")
	}
	cmd = append(cmd, a.archivePath())
	cmd = append(cmd, "-") // write the tar to stdout

	exitCode, stderr, err := a.Repository.Container.ExecStream(ctx, []string{"sh", "-c", strings.Join(cmd, " ")}, w)
	if err != nil {
		return &LogMessage{Message: err.Error()}
	}
	if exitCode != 0 {
		// The exit code is authoritative. Prefer borg's structured error.
		if log := readArchiveRestoreResponse(stderr); log != nil {
			return log
		}
		return &LogMessage{Message: "borg export-tar exited with code " + strconv.Itoa(exitCode)}
	}
	return nil
}
