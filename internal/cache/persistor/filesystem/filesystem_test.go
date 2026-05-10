package filesystem

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveAll_OnlyDeletesCacheEntries(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	require.NoError(t, err)
	t.Cleanup(cleanup)

	p, err := New(*tempDir)
	require.NoError(t, err)

	cacheFile := filepath.Join(*tempDir, "abc.cache")
	nonCacheFile := filepath.Join(*tempDir, "keep.txt")
	subDir := filepath.Join(*tempDir, "sub")
	subDirCacheFile := filepath.Join(subDir, "nested.cache")

	require.NoError(t, os.WriteFile(cacheFile, []byte("cache"), 0o600))
	require.NoError(t, os.WriteFile(nonCacheFile, []byte("keep"), 0o600))
	require.NoError(t, os.MkdirAll(subDir, 0o755))
	require.NoError(t, os.WriteFile(subDirCacheFile, []byte("nested"), 0o600))

	require.NoError(t, p.RemoveAll())

	_, err = os.Stat(cacheFile)
	assert.ErrorIs(t, err, os.ErrNotExist)

	_, err = os.Stat(nonCacheFile)
	assert.NoError(t, err)

	_, err = os.Stat(subDir)
	assert.NoError(t, err)

	_, err = os.Stat(subDirCacheFile)
	assert.NoError(t, err)
}
