package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	partOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/partoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type rejectFinalizeRepository struct {
	partOutboxEntry.Repository
}

func (r *rejectFinalizeRepository) DeletePartOutboxEntryByClaimOwner(context.Context, *sql.Tx, string, ulid.ULID, string) (bool, error) {
	return false, nil
}

func TestLostClaimLeavesInnerPartMutationForIdempotentReplay(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()

	storagePath := t.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(storagePath, "pithos.db"))
	require.NoError(t, err)
	defer db.Close()

	inner, err := filesystemPartStore.New(filepath.Join(storagePath, "parts"))
	require.NoError(t, err)
	require.NoError(t, inner.Start(ctx))
	defer inner.Stop(ctx)

	repo, err := repositoryFactory.NewPartOutboxEntryRepository(db)
	require.NoError(t, err)
	rejectingRepo := &rejectFinalizeRepository{Repository: repo}
	store, err := New(db, "default", inner, rejectingRepo, prometheus.NewRegistry(), 30*time.Second)
	require.NoError(t, err)
	obs := store.(*outboxPartStore)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return obs.PutPart(ctx, tx, *partId, bytes.NewReader([]byte("must not be published")))
	}))

	obs.maybeProcessOutboxEntries(ctx)

	reader, err := inner.GetPart(ctx, nil, *partId)
	require.NoError(t, err)
	defer reader.Close()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("must not be published"), content)
}
