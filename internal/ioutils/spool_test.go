package ioutils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSpoolFileUsesConfiguredDirectory(t *testing.T) {
	spoolDir := t.TempDir()
	t.Setenv(spoolDirEnvKey, spoolDir)

	file, err := CreateSpoolFile("configured-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		file.Close()
		os.Remove(file.Name())
	})

	assert.Equal(t, spoolDir, filepath.Dir(file.Name()))
}

func TestSetSpoolDirOverridesEnvironment(t *testing.T) {
	spoolDir := t.TempDir()
	t.Setenv(spoolDirEnvKey, t.TempDir())
	SetSpoolDir(spoolDir)
	t.Cleanup(func() {
		configuredSpoolDir.Store(nil)
	})

	file, err := CreateSpoolFile("settings-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		file.Close()
		os.Remove(file.Name())
	})

	assert.Equal(t, spoolDir, filepath.Dir(file.Name()))
}

func TestCreateSpoolFileFallsBackToPlatformTempDirectory(t *testing.T) {
	t.Setenv(spoolDirEnvKey, "")

	file, err := CreateSpoolFile("fallback-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		file.Close()
		os.Remove(file.Name())
	})

	assert.Equal(t, filepath.Clean(os.TempDir()), filepath.Dir(file.Name()))
}

func TestCreateSpoolFileReportsConfiguredDirectory(t *testing.T) {
	spoolDir := filepath.Join(t.TempDir(), "missing")
	t.Setenv(spoolDirEnvKey, spoolDir)

	file, err := CreateSpoolFile("missing-*")

	assert.Nil(t, file)
	require.Error(t, err)
	assert.Contains(t, err.Error(), spoolDir)
}

func TestDiskCachedReadSeekCloserUsesSpoolDirectoryAndRemovesFile(t *testing.T) {
	spoolDir := t.TempDir()
	t.Setenv(spoolDirEnvKey, spoolDir)

	reader, err := NewDiskCachedReadSeekCloser(strings.NewReader("spooled content"))
	require.NoError(t, err)

	cachedReader, ok := reader.(*diskCachedReadSeekCloser)
	require.True(t, ok)
	path := cachedReader.f.Name()
	assert.Equal(t, spoolDir, filepath.Dir(path))
	_, err = os.Stat(path)
	require.NoError(t, err)

	require.NoError(t, reader.Close())
	_, err = os.Stat(path)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskCachedReadSeekCloserRemovesFileAfterCopyError(t *testing.T) {
	spoolDir := t.TempDir()
	t.Setenv(spoolDirEnvKey, spoolDir)

	reader, err := NewDiskCachedReadSeekCloser(iotest.ErrReader(assert.AnError))
	require.Error(t, err)
	assert.Nil(t, reader)

	files, err := filepath.Glob(filepath.Join(spoolDir, diskCachePattern))
	require.NoError(t, err)
	assert.Empty(t, files)
}
