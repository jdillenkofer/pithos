package outbox

import (
	"path/filepath"
	"testing"
	"time"

	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestOutboxPartStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath := t.TempDir()
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	filesystemPartStore, err := filesystemPartStore.New(storagePath)
	require.NoError(t, err)
	partOutboxEntryRepository, err := repositoryFactory.NewPartOutboxEntryRepository(db)
	require.NoError(t, err)
	reg := prometheus.NewRegistry()
	outboxPartStore, err := New(db, "default", filesystemPartStore, partOutboxEntryRepository, reg, 30*time.Second)
	require.NoError(t, err)
	content := []byte("OutboxPartStore")
	require.NoError(t, partstore.Tester(outboxPartStore, db, content))
}
