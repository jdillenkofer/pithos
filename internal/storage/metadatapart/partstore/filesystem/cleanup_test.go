package filesystem

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestStartupCleansOnlyAbandonedTemporaryParts(t *testing.T) {
	testutils.SkipIfIntegration(t)
	root := t.TempDir()
	const id = "0123456789abcdef0123456789abcdef"
	temporary := "." + id + ".tmp.01ARZ3NDEKTSV4RRFFQ69G5FAV"
	preserved := []string{id, id + ".txbackup.old", ".unrelated.tmp"}
	for _, name := range append([]string{temporary}, preserved...) {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte("data"), 0o600))
	}
	// Even a matching directory is not an upload file.
	directory := filepath.Join(root, "."+id+".tmp.01ARZ3NDEKTSV4RRFFQ69G5FAW")
	require.NoError(t, os.Mkdir(directory, 0o700))

	store, err := New(root)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, store.Start(ctx))
	t.Cleanup(func() { _ = store.Stop(ctx) })
	require.NoFileExists(t, filepath.Join(root, temporary))
	for _, name := range preserved {
		require.FileExists(t, filepath.Join(root, name))
	}
	require.DirExists(t, directory)
}
