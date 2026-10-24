package sftp

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/pkg/sftp"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestStartupCleansOnlyAbandonedTemporaryParts(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	addr, config := prepareSshServer(t, false)
	ps, err := New(addr, config, "/tmp/pithos")
	require.NoError(t, err)
	require.NoError(t, ps.Start(ctx))
	t.Cleanup(func() { _ = ps.Stop(ctx) })
	store := ps.(*sftpPartStore)
	const id = "0123456789abcdef0123456789abcdef"
	temporary := "." + id + ".tmp.01ARZ3NDEKTSV4RRFFQ69G5FAV"
	preserved := []string{id, id + ".txbackup.old", ".unrelated.tmp"}
	require.NoError(t, store.withClient(func(client *sftp.Client) error {
		for _, name := range append([]string{temporary}, preserved...) {
			file, err := client.OpenFile(filepath.Join(store.root, name), os.O_CREATE|os.O_WRONLY)
			if err != nil {
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, ps.Stop(ctx))
	// A new process would see the files left by its predecessor in this state.
	require.NoError(t, ps.Start(ctx))
	require.NoError(t, store.withClient(func(client *sftp.Client) error {
		entries, err := client.ReadDir(store.root)
		if err != nil {
			return err
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		require.ElementsMatch(t, preserved, names)
		return nil
	}))
}
