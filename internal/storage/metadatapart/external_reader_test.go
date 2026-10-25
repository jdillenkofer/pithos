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
	"github.com/stretchr/testify/require"
)

// Deletion removes metadata immediately, but physical content and zero-reference
// registry rows must survive for existing bounded readers.
func assertRetainedUnreferencedParts(t *testing.T, st *metadataPartStorage) {
	t.Helper()
	require.NotEmpty(t, physicalPartIDs(t, st))
	require.NoError(t, database.WithTx(context.Background(), st.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var live, missingTime int
		require.NoError(t, tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM parts").Scan(&live))
		require.Zero(t, live)
		require.NoError(t, tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM part_registry WHERE ref_count != 0 OR unreferenced_at IS NULL").Scan(&missingTime))
		require.Zero(t, missingTime)
		return nil
	}))
}

func TestExternalGetShortTransactionLazyDeleteAndSharedDeadline(t *testing.T) {
	st, cleanup := newTestStorage(t)
	defer cleanup()
	ctx := context.Background()
	fs, err := filesystem.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, fs.Start(ctx))
	defer fs.Stop(ctx)
	original := st.partStores
	defer func() { st.partStores = original }()
	st.partStores, err = partstore.NewNamedPartStores(fs, nil, nil)
	require.NoError(t, err)
	bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err = st.PutObject(ctx, bucket, key, nil, ioutils.NewByteReadSeekCloser([]byte("abcdef")), nil, nil)
	require.NoError(t, err)
	zero, two, four, six := int64(0), int64(2), int64(4), int64(6)
	// No part is opened until Read; both ranges share exactly one deadline.
	_, readers, err := st.GetObject(ctx, bucket, key, []storage.ByteRange{{Start: &zero, End: &two}, {Start: &four, End: &six}}, nil)
	require.NoError(t, err)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	first := readers[0].(interface{ Deadline() time.Time }).Deadline()
	require.Equal(t, first, readers[1].(interface{ Deadline() time.Time }).Deadline())
	// A metadata writer can complete while the lazy external readers are idle.
	_, err = st.DeleteObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	for i, want := range []string{"ab", "ef"} {
		data, err := io.ReadAll(readers[i])
		require.NoError(t, err)
		require.Equal(t, want, string(data))
		require.NoError(t, readers[i].Close())
		require.NoError(t, readers[i].Close()) // Must not cancel the other ranges twice.
	}
	// Reusing an older ambient snapshot cannot safely establish a new lifetime.
	require.NoError(t, st.WithTransaction(ctx, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, _ storage.Storage) error {
		_, _, err := st.GetObject(ctx, bucket, key, nil, nil)
		require.ErrorContains(t, err, "ambient transaction")
		return nil
	}))
}
