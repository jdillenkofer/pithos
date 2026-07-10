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
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOutboxGetPartTxFree covers GetPart with a nil transaction, both while
// the part is still pending in the outbox and after it has been flushed to
// the inner store.
func TestOutboxGetPartTxFree(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()

	storagePath := t.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(storagePath, "pithos.db"))
	require.NoError(t, err)
	defer db.Close()

	inner, err := filesystemPartStore.New(storagePath)
	require.NoError(t, err)
	partOutboxEntryRepository, err := repositoryFactory.NewPartOutboxEntryRepository(db)
	require.NoError(t, err)
	outboxStore, err := New(db, "default", inner, partOutboxEntryRepository, prometheus.NewRegistry(), 30*time.Second)
	require.NoError(t, err)

	assert.True(t, partstore.SupportsTxFreeGetPart(outboxStore))

	content := []byte("tx-free outbox part content")
	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)

	// Write the part into the outbox but do not start the store, so the
	// entry stays pending and must be served from the DB.
	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return outboxStore.PutPart(ctx, tx, *partId, bytes.NewReader(content))
	})
	require.NoError(t, err)

	rc, err := outboxStore.GetPart(ctx, nil, *partId)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, content, got)

	// A pending empty put must be authoritative even when older content still
	// exists in the inner store.
	emptyPartId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, inner.PutPart(ctx, nil, *emptyPartId, bytes.NewReader([]byte("stale content"))))
	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return outboxStore.PutPart(ctx, tx, *emptyPartId, bytes.NewReader(nil))
	})
	require.NoError(t, err)

	emptyReader, err := outboxStore.GetPart(ctx, nil, *emptyPartId)
	require.NoError(t, err)
	emptyContent, err := io.ReadAll(emptyReader)
	require.NoError(t, err)
	require.NoError(t, emptyReader.Close())
	assert.Empty(t, emptyContent)

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		emptyReader, err := outboxStore.GetPart(ctx, tx, *emptyPartId)
		if err != nil {
			return err
		}
		defer emptyReader.Close()
		emptyContent, err := io.ReadAll(emptyReader)
		if err != nil {
			return err
		}
		assert.Empty(t, emptyContent)
		return nil
	})
	require.NoError(t, err)

	// Start the store so the outbox flushes to the inner store, then the
	// tx-free read must be served from the inner store.
	require.NoError(t, outboxStore.Start(ctx))
	defer outboxStore.Stop(ctx)
	require.Eventually(t, func() bool {
		ids, err := inner.GetPartIds(ctx, nil)
		return err == nil && len(ids) == 2
	}, 10*time.Second, 50*time.Millisecond)

	rc2, err := outboxStore.GetPart(ctx, nil, *partId)
	require.NoError(t, err)
	got2, err := io.ReadAll(rc2)
	require.NoError(t, err)
	require.NoError(t, rc2.Close())
	assert.Equal(t, content, got2)

	// Missing parts surface ErrPartNotFound without a transaction, too.
	missingId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	_, err = outboxStore.GetPart(ctx, nil, *missingId)
	assert.ErrorIs(t, err, partstore.ErrPartNotFound)
}

func TestOutboxCommitAfterStopDoesNotPanic(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()

	storagePath := t.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(storagePath, "pithos.db"))
	require.NoError(t, err)
	defer db.Close()

	inner, err := filesystemPartStore.New(storagePath)
	require.NoError(t, err)
	repo, err := repositoryFactory.NewPartOutboxEntryRepository(db)
	require.NoError(t, err)
	outboxStore, err := New(db, "default", inner, repo, prometheus.NewRegistry(), 30*time.Second)
	require.NoError(t, err)

	require.NoError(t, outboxStore.Start(ctx))
	require.Error(t, outboxStore.Start(ctx))
	require.NoError(t, outboxStore.Stop(ctx))
	require.Error(t, outboxStore.Stop(ctx))

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return outboxStore.PutPart(ctx, tx, *partId, bytes.NewReader([]byte("committed after stop")))
	}))
}
