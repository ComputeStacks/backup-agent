package job

import (
	"cs-agent/types"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestResolveArchiveName(t *testing.T) {
	tests := []struct {
		name     string
		job      *types.Job
		expected string
	}{
		{
			name: "Empty ArchiveName",
			job: &types.Job{
				ArchiveName: "",
			},
			expected: "manual-m-{utcnow}",
		},
		{
			name: "Auto ArchiveName",
			job: &types.Job{
				ArchiveName: "auto",
			},
			expected: "auto-{utcnow}",
		},
		{
			name: "Custom ArchiveName",
			job: &types.Job{
				ArchiveName: "custom",
			},
			expected: "custom-m-{utcnow}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolveArchiveName(tt.job)
			assert.Equal(t, tt.expected, tt.job.ArchiveName)
		})
	}
}

func TestMain(m *testing.M) {
	viper.SetDefault("queue.numworkers", 2)
	m.Run()
}
