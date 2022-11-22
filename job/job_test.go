package job

import (
	"github.com/spf13/viper"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	viper.SetDefault("queue.numworkers", 2)
	exitCode := m.Run()
	os.Exit(exitCode)
}