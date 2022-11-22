package borg

import "testing"

var pathChecks = []struct {
	in  string
	out string
}{
	{"/path/to/something", "/path/to/something/"},
	{"/path/to/something/", "/path/to/something/"},
}

func TestBackupPath(t *testing.T) {
	for _, i := range pathChecks {
		t.Run(i.in, func(t *testing.T) {
			result := forceTrailingSlash(i.in)
			if result != i.out {
				t.Errorf("Received %q, wanted %q", result, i.out)
			}
		})
	}

}
