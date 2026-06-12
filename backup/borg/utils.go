package borg

import (
	"cs-agent/log"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
)

// Docker volume / borg repository names: must start alphanumeric, then the
// Docker volume name charset. Used to guard names interpolated into remote
// shell commands (e.g. NFS-server compact over SSH).
var repoNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// safeRepoName reports whether name is a plain repository identifier safe to
// interpolate into a remote shell command.
func safeRepoName(name string) bool {
	return repoNameRe.MatchString(name)
}

type BTimeFormat time.Time

func (bt *BTimeFormat) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	t, err := time.Parse("2006-01-02T15:04:05.999999", s)
	if err != nil {
		return err
	}
	*bt = BTimeFormat(t)
	return nil
}

func CurrentPath() string {
	// Configure our path
	_, filename, _, _ := runtime.Caller(0)
	p := strings.Split(filename, "/")
	p = p[:len(p)-1]
	return strings.Join(p, "/")
}

// .LastModified.Format("2006-01-02 15:04:05")
func (bt BTimeFormat) Format(s string) string {
	t := time.Time(bt)
	return t.Format(s)
}

func borgLogger() hclog.Logger {
	return log.New().Named("borg")
}

func forceTrailingSlash(s string) string {
	fullPath := s
	if string(fullPath[len(fullPath)-1:]) != "/" {
		fullPath = fullPath + "/"
	}
	return fullPath
}
