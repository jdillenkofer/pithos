package ioutils

import (
	"fmt"
	"os"
	"sync/atomic"
)

const spoolDirEnvKey = "PITHOS_SPOOL_DIR"

var configuredSpoolDir atomic.Pointer[string]

// SetSpoolDir configures the process-wide directory used for spool files.
// It should be called during application startup, before spool files can be
// created.
func SetSpoolDir(dir string) {
	configuredSpoolDir.Store(&dir)
}

func spoolDir() string {
	if dir := configuredSpoolDir.Load(); dir != nil {
		return *dir
	}
	return os.Getenv(spoolDirEnvKey)
}

// CreateSpoolFile creates a temporary file in the configured spool directory.
// Without application configuration it also recognizes PITHOS_SPOOL_DIR,
// allowing commands that do not load server settings to use the same setting.
// An empty value preserves os.CreateTemp's platform-default behavior.
func CreateSpoolFile(pattern string) (*os.File, error) {
	dir := spoolDir()
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		if dir == "" {
			dir = os.TempDir()
		}
		return nil, fmt.Errorf("create spool file in %q: %w", dir, err)
	}
	return file, nil
}
