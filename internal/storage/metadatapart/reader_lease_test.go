package metadatapart

import (
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

// newExternalLeaseTestStorage serves all content from a filesystem store with
// a lease-free window short enough to exercise leases without long sleeps.
func newExternalLeaseTestStorage(t *testing.T, backend string, window time.Duration) (*metadataPartStorage, func()) {
	t.Helper()
	var st *metadataPartStorage
	var cleanup func()
	if backend == "postgres" {
		st, cleanup = newPostgresTestStorage(t)
	} else {
		st, cleanup = newTestStorage(t)
	}
	ctx := context.Background()
	fs, err := filesystem.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, fs.Start(ctx))
	original := st.partStores
	st.partStores, err = partstore.NewNamedPartStores(fs, nil, nil)
	require.NoError(t, err)
	st.leaseFreeWindow = window
	st.readerLeaseTTL = window
	return st, func() {
		st.partStores = original
		require.NoError(t, fs.Stop(ctx))
		cleanup()
	}
}

func countReaderLeases(t *testing.T, st *metadataPartStorage) int {
	t.Helper()
	var leases int
	require.NoError(t, database.WithTx(context.Background(), st.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		return tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM part_reader_lease").Scan(&leases)
	}))
	return leases
}

// condemnRetained ages the GC observation of every retained part beyond any
// cutoff, so only a reader lease can still keep it alive.
func condemnRetained(t *testing.T, st *metadataPartStorage, ids []partstore.PartId) bool {
	t.Helper()
	condemnedAll := true
	require.NoError(t, database.WithTx(context.Background(), st.db, nil, func(ctx context.Context, tx database.Tx) error {
		for _, id := range ids {
			_, err := tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET unreferenced_at = $1 WHERE part_id = $2", time.Now().UTC().Add(-2*time.Hour), id.String())
			require.NoError(t, err)
			ok, err := st.partRegistryRepository.Condemn(ctx, tx.SqlTx(), id, time.Now().UTC().Add(-time.Hour))
			require.NoError(t, err)
			condemnedAll = condemnedAll && ok
		}
		return nil
	}))
	return condemnedAll
}

func TestExternalGetOutlivesLeaseFreeWindow(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			window := 200 * time.Millisecond
			st, cleanup := newExternalLeaseTestStorage(t, backend, window)
			defer cleanup()
			ctx := context.Background()
			bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			_, err := st.PutObject(ctx, bucket, key, nil, ioutils.NewByteReadSeekCloser([]byte("long download")), nil, nil)
			require.NoError(t, err)
			ids := physicalPartIDs(t, st)
			require.Len(t, ids, 1)

			_, readers, err := st.GetObject(ctx, bucket, key, nil, nil)
			require.NoError(t, err)
			prefix := make([]byte, 4)
			_, err = io.ReadFull(readers[0], prefix)
			require.NoError(t, err)
			_, err = st.DeleteObject(ctx, bucket, key, nil)
			require.NoError(t, err)

			require.Eventually(t, func() bool { return countReaderLeases(t, st) == 1 }, 5*time.Second, 10*time.Millisecond)
			require.False(t, condemnRetained(t, st, ids), "a leased part must survive retention")
			// Several TTLs later the lease is still renewed and still protects.
			time.Sleep(3 * window)
			require.False(t, condemnRetained(t, st, ids))
			rest, err := io.ReadAll(readers[0])
			require.NoError(t, err)
			require.Equal(t, "long download", string(prefix)+string(rest))

			require.NoError(t, readers[0].Close())
			require.Zero(t, countReaderLeases(t, st), "the last Close releases the lease")
			require.True(t, condemnRetained(t, st, ids))
		})
	}
}

func TestShortExternalGetWritesNoLease(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, cleanup := newExternalLeaseTestStorage(t, "sqlite", time.Minute)
	defer cleanup()
	ctx := context.Background()
	bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err := st.PutObject(ctx, bucket, key, nil, ioutils.NewByteReadSeekCloser([]byte("abcdef")), nil, nil)
	require.NoError(t, err)
	zero, two, four, six := int64(0), int64(2), int64(4), int64(6)
	_, readers, err := st.GetObject(ctx, bucket, key, []storage.ByteRange{{Start: &zero, End: &two}, {Start: &four, End: &six}}, nil)
	require.NoError(t, err)
	for i, want := range []string{"ab", "ef"} {
		data, err := io.ReadAll(readers[i])
		require.NoError(t, err)
		require.Equal(t, want, string(data))
		require.NoError(t, readers[i].Close())
		require.NoError(t, readers[i].Close())
	}
	require.Zero(t, countReaderLeases(t, st))
}

func TestExternalGetStopsWhenContentWasCondemnedBeforeLease(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			window := 200 * time.Millisecond
			st, cleanup := newExternalLeaseTestStorage(t, backend, window)
			defer cleanup()
			ctx := context.Background()
			bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			_, err := st.PutObject(ctx, bucket, key, nil, ioutils.NewByteReadSeekCloser([]byte("content")), nil, nil)
			require.NoError(t, err)
			ids := physicalPartIDs(t, st)
			_, readers, err := st.GetObject(ctx, bucket, key, nil, nil)
			require.NoError(t, err)
			defer readers[0].Close()
			_, err = st.DeleteObject(ctx, bucket, key, nil)
			require.NoError(t, err)
			// Simulate a collector that violated retention (e.g. a skewed clock)
			// before the reader could acquire its lease.
			require.True(t, condemnRetained(t, st, ids))
			require.Eventually(t, func() bool {
				_, err := readers[0].Read(make([]byte, 1))
				return err != nil && err != io.EOF
			}, 5*time.Second, 10*time.Millisecond)
			_, err = readers[0].Read(make([]byte, 1))
			require.ErrorIs(t, err, errReaderLeaseCondemned)
		})
	}
}

func TestExternalGetInAmbientTransactionIsNotLeased(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, cleanup := newExternalLeaseTestStorage(t, "sqlite", 200*time.Millisecond)
	defer cleanup()
	ctx := context.Background()
	bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err := st.PutObject(ctx, bucket, key, nil, ioutils.NewByteReadSeekCloser([]byte("content")), nil, nil)
	require.NoError(t, err)
	// An older ambient snapshot cannot anchor the lease-free window. Callers
	// such as replication journaling read within their own transaction.
	require.NoError(t, st.WithTransaction(ctx, nil, func(ctx context.Context, _ storage.Storage) error {
		_, readers, err := st.GetObject(ctx, bucket, key, nil, nil)
		require.NoError(t, err)
		defer readers[0].Close()
		_, isLeased := readers[0].(*leasedReadCloser)
		require.False(t, isLeased)
		data, err := io.ReadAll(readers[0])
		require.NoError(t, err)
		require.Equal(t, "content", string(data))
		return nil
	}))
	require.Zero(t, countReaderLeases(t, st))
}
