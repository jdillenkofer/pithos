package ioutils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "entry"), []byte("data"), 0o600))
	require.NoError(t, SyncDirectory(dir))
	require.Error(t, SyncDirectory(filepath.Join(dir, "missing")))
}
