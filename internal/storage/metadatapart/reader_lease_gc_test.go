package metadatapart

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/gc"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/readerlease"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

// useShortReaderLeases swaps the storage's reader leases for ones derived
// from graceWindow and returns a collector that honors them.
func useShortReaderLeases(t *testing.T, st *metadataPartStorage, graceWindow time.Duration) gc.PartGarbageCollector {
	t.Helper()
	stopStorageGC(t, st)
	window := leaseFreeWindowFor(graceWindow)
	store, err := readerlease.New(st.db, window)
	require.NoError(t, err)
	st.readerLeases.shutdown(context.Background())
	st.readerLeases = newReaderLeases(store, window)
	st.readerLeases.start()
	registry, err := repositoryfactory.NewPartRegistryRepository(st.db)
	require.NoError(t, err)
	dedup, err := repositoryfactory.NewPartDedupIndexRepository(st.db)
	require.NoError(t, err)
	collector, err := gc.New(st.db, st.metadataStore, st.partStores, registry, dedup, store, graceWindow, 10*time.Millisecond)
	require.NoError(t, err)
	return collector
}

// A download that outlives the grace window keeps reading the content of an
// object deleted meanwhile, and GC reclaims the content once it is closed.
func TestReaderLeaseProtectsDownloadsOfDeletedObjectsFromGC(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var st *metadataPartStorage
			var cleanup func()
			if backend == "postgres" {
				st, cleanup = newPostgresTestStorage(t)
			} else {
				st, cleanup = newTestStorage(t)
			}
			defer cleanup()
			graceWindow := 600 * time.Millisecond
			collector := useShortReaderLeases(t, st, graceWindow)
			ctx := context.Background()

			bucket := storage.MustNewBucketName("bucket")
			key := storage.MustNewObjectKey("key")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			content := strings.Repeat("pithos", 1024)
			_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte(content)), nil, nil)
			require.NoError(t, err)

			_, readers, err := st.GetObject(ctx, bucket, key, nil, nil)
			require.NoError(t, err)
			require.Len(t, readers, 1)
			reader := readers[0]
			defer reader.Close()
			head := make([]byte, 6)
			_, err = io.ReadFull(reader, head)
			require.NoError(t, err)

			_, err = st.DeleteObject(ctx, bucket, key, nil)
			require.NoError(t, err)
			var stop atomic.Bool
			done := make(chan struct{})
			go func() { defer close(done); collector.RunGCLoop(&stop) }()
			defer func() { stop.Store(true); <-done }()

			time.Sleep(3 * graceWindow)
			require.Len(t, physicalPartIDs(t, st), 1)
			rest, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, content, string(head)+string(rest))

			require.NoError(t, reader.Close())
			require.Eventually(t, func() bool { return len(physicalPartIDs(t, st)) == 0 }, 5*time.Second, 10*time.Millisecond)
		})
	}
}

// A transaction that reads one snapshot cannot stream parts once it is
// older than the lease-free window, even parts metadata still references.
func TestOldSnapshotCannotReadParts(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var st *metadataPartStorage
			var cleanup func()
			txOptions := &sql.TxOptions{ReadOnly: true}
			if backend == "postgres" {
				st, cleanup = newPostgresTestStorage(t)
				txOptions.Isolation = sql.LevelRepeatableRead
			} else {
				st, cleanup = newTestStorage(t)
			}
			defer cleanup()
			graceWindow := 600 * time.Millisecond
			useShortReaderLeases(t, st, graceWindow)
			ctx := context.Background()

			bucket := storage.MustNewBucketName("bucket")
			key := storage.MustNewObjectKey("key")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("pithos")), nil, nil)
			require.NoError(t, err)

			err = st.WithTransaction(ctx, txOptions, func(txCtx context.Context, txStorage storage.Storage) error {
				// The first statement takes the snapshot.
				_, err := txStorage.HeadObject(txCtx, bucket, key, nil)
				require.NoError(t, err)
				time.Sleep(leaseFreeWindowFor(graceWindow))

				_, readers, err := txStorage.GetObject(txCtx, bucket, key, nil, nil)
				require.NoError(t, err)
				_, err = io.ReadAll(readers[0])
				require.ErrorIs(t, err, errSnapshotTooOld)
				return readers[0].Close()
			})
			require.NoError(t, err)
		})
	}
}

// Statements of a writable transaction read the latest committed state, so
// however long it has been open, it reads parts it wrote itself.
func TestOldWritableTransactionReadsItsOwnWrites(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var st *metadataPartStorage
			var cleanup func()
			if backend == "postgres" {
				st, cleanup = newPostgresTestStorage(t)
			} else {
				st, cleanup = newTestStorage(t)
			}
			defer cleanup()
			graceWindow := 600 * time.Millisecond
			useShortReaderLeases(t, st, graceWindow)
			ctx := context.Background()

			bucket := storage.MustNewBucketName("bucket")
			key := storage.MustNewObjectKey("key")
			require.NoError(t, st.CreateBucket(ctx, bucket))

			err := st.WithTransaction(ctx, nil, func(txCtx context.Context, txStorage storage.Storage) error {
				_, err := txStorage.HeadBucket(txCtx, bucket)
				require.NoError(t, err)
				time.Sleep(leaseFreeWindowFor(graceWindow))

				_, err = txStorage.PutObject(txCtx, bucket, key, nil, bytes.NewReader([]byte("pithos")), nil, nil)
				require.NoError(t, err)
				_, readers, err := txStorage.GetObject(txCtx, bucket, key, nil, nil)
				require.NoError(t, err)
				body, err := io.ReadAll(readers[0])
				require.NoError(t, err)
				require.Equal(t, "pithos", string(body))
				return readers[0].Close()
			})
			require.NoError(t, err)
		})
	}
}
