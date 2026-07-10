package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"sync/atomic"
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

type countingChunkRepository struct {
	partOutboxEntry.Repository
	bulkReads  atomic.Int32
	chunkReads atomic.Int32
}

func (r *countingChunkRepository) FindPartOutboxEntryChunksById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) ([]*partOutboxEntry.ContentChunk, error) {
	r.bulkReads.Add(1)
	return r.Repository.FindPartOutboxEntryChunksById(ctx, tx, outboxId, id)
}

func (r *countingChunkRepository) FindPartOutboxEntryChunkByIndexWithEntryPresence(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, chunkIndex int) (*partOutboxEntry.ContentChunk, bool, error) {
	r.chunkReads.Add(1)
	return r.Repository.FindPartOutboxEntryChunkByIndexWithEntryPresence(ctx, tx, outboxId, id, chunkIndex)
}

func TestReplayStreamsOutboxChunks(t *testing.T) {
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
	countingRepo := &countingChunkRepository{Repository: repo}
	store, err := New(db, "default", inner, countingRepo, prometheus.NewRegistry(), 30*time.Second)
	require.NoError(t, err)
	obs := store.(*outboxPartStore)

	content := bytes.Repeat([]byte("x"), chunkSize+1)
	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return obs.PutPart(ctx, tx, *partId, bytes.NewReader(content))
	}))

	obs.maybeProcessOutboxEntries(ctx)

	require.Zero(t, countingRepo.bulkReads.Load())
	require.EqualValues(t, 3, countingRepo.chunkReads.Load()) // chunks 0, 1, then EOF at 2
	reader, err := inner.GetPart(ctx, nil, *partId)
	require.NoError(t, err)
	defer reader.Close()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

// TestGetPartReaderFallsBackToInnerStoreWhenEntryFlushedMidRead reproduces the
// race where the outbox worker flushes and deletes an entry while a reader is
// still streaming its chunks. Under statement-level isolation (Postgres READ
// COMMITTED) the reader's next chunk query then finds neither the chunk nor
// the entry; it must continue from the inner store instead of reporting a
// truncated (or empty) part.
func TestGetPartReaderFallsBackToInnerStoreWhenEntryFlushedMidRead(t *testing.T) {
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
	store, err := New(db, "default", inner, repo, prometheus.NewRegistry(), 30*time.Second)
	require.NoError(t, err)
	obs := store.(*outboxPartStore)

	chunk0 := []byte("hello")
	chunk1 := []byte("world")
	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	var entryId ulid.ULID
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		entry := partOutboxEntry.Entity{Operation: partOutboxEntry.PutPartOperation, PartId: *partId}
		if err := repo.SavePartOutboxEntry(ctx, tx.SqlTx(), obs.outboxId, &entry); err != nil {
			return err
		}
		entryId = *entry.Id
		for i, content := range [][]byte{chunk0, chunk1} {
			chunk := partOutboxEntry.ContentChunk{OutboxEntryId: entryId, ChunkIndex: i, Content: content}
			if err := repo.SavePartOutboxContentChunk(ctx, tx.SqlTx(), &chunk); err != nil {
				return err
			}
		}
		return nil
	}))

	// Flush the entry to the inner store; this deletes the entry and its chunks.
	obs.maybeProcessOutboxEntries(ctx)

	// Simulate a reader that observed the entry (and consumed chunk 0) before
	// the flush: its next chunk query no longer finds the entry.
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		reader := &lazyOutboxChunkReadCloser{
			ctx:       ctx,
			tx:        tx,
			repo:      repo,
			outboxId:  obs.outboxId,
			entryId:   entryId,
			nextChunk: 1,
			current:   bytes.NewReader(chunk0),
			inner:     inner,
			partId:    *partId,
		}
		defer reader.Close()
		got, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, []byte("helloworld"), got)
		return nil
	}))
}

// TestReplayReaderFailsWhenEntryVanishes ensures the replay worker's own
// reader (which has no inner-store fallback) surfaces an error instead of
// silently truncating when the entry is deleted out from under it, e.g. by a
// lease takeover.
func TestReplayReaderFailsWhenEntryVanishes(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()

	storagePath := t.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(storagePath, "pithos.db"))
	require.NoError(t, err)
	defer db.Close()

	repo, err := repositoryFactory.NewPartOutboxEntryRepository(db)
	require.NoError(t, err)

	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		reader := &lazyOutboxChunkReadCloser{
			ctx:      ctx,
			tx:       tx,
			repo:     repo,
			outboxId: "default",
			entryId:  ulid.Make(),
		}
		defer reader.Close()
		_, err := io.ReadAll(reader)
		require.ErrorIs(t, err, errPartOutboxEntryVanished)
		return nil
	}))
}
