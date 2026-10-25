package metadatapart

import (
	"context"
	"database/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"io"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	"github.com/stretchr/testify/require"
)

// A reader can see the old committed manifest after the deleting transaction's
// timestamp has aged past retention. Only a subsequent committed-zero
// observation can establish a safe retention clock (MVCC and WAL alike).
func TestExternalGetDuringDelayedMutationCommit(t *testing.T) {
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
			ctx := context.Background()
			fs, err := filesystem.New(t.TempDir())
			require.NoError(t, err)
			require.NoError(t, fs.Start(ctx))
			original := st.partStores
			defer func() { st.partStores = original; require.NoError(t, fs.Stop(ctx)) }()
			st.partStores, err = partstore.NewNamedPartStores(fs, nil, nil)
			require.NoError(t, err)
			st.leaseFreeWindow = 100 * time.Millisecond
			repo, err := repositoryfactory.NewPartRegistryRepository(st.db)
			require.NoError(t, err)
			bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			_, err = st.PutObject(ctx, bucket, key, nil, ioutils.NewByteReadSeekCloser([]byte("old manifest")), nil, nil)
			require.NoError(t, err)
			ids := physicalPartIDs(t, st)
			require.Len(t, ids, 1)
			mutated, commit := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- st.WithTransaction(ctx, nil, func(ctx context.Context, s storage.Storage) error {
					if _, err := s.DeleteObject(ctx, bucket, key, nil); err != nil {
						return err
					}
					close(mutated)
					<-commit
					return nil
				})
			}()
			<-mutated
			// Deliberately age the mutation beyond R before the independent GET starts.
			time.Sleep(250 * time.Millisecond)
			readerStart := time.Now().UTC()
			_, readers, err := st.GetObject(ctx, bucket, key, nil, nil)
			close(commit)
			require.NoError(t, <-done)
			require.NoError(t, err)
			defer readers[0].Close()
			require.NoError(t, database.WithTx(ctx, st.db, nil, func(ctx context.Context, tx database.Tx) error {
				rows, err := repo.FindAllEntities(ctx, tx.SqlTx())
				require.NoError(t, err)
				require.Nil(t, rows[0].UnreferencedAt, "mutation must not start retention")
				ok, err := repo.Condemn(ctx, tx.SqlTx(), ids[0], time.Now().UTC().Add(-200*time.Millisecond))
				require.NoError(t, err)
				require.False(t, ok, "new reader must survive GC immediately after delayed commit")
				rows, err = repo.FindAllEntities(ctx, tx.SqlTx())
				require.NoError(t, err)
				require.False(t, rows[0].UnreferencedAt.Before(readerStart))
				// Reconciliation must preserve the observation, not the mutation clock.
				observed := *rows[0].UnreferencedAt
				ok, err = repo.UpdateRefCount(ctx, tx.SqlTx(), ids[0], 0, rows[0].Version)
				require.NoError(t, err)
				require.True(t, ok)
				rows, err = repo.FindAllEntities(ctx, tx.SqlTx())
				require.NoError(t, err)
				require.True(t, observed.Equal(*rows[0].UnreferencedAt))
				return nil
			}))
			data, err := io.ReadAll(readers[0])
			require.NoError(t, err)
			require.Equal(t, "old manifest", string(data))
			require.NoError(t, readers[0].Close())
			// Only a later collection, with an elapsed observation interval, condemns.
			require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{}, func(ctx context.Context, tx database.Tx) error {
				_, err := tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET unreferenced_at=$1 WHERE part_id=$2", time.Now().UTC().Add(-time.Hour), ids[0].String())
				require.NoError(t, err)
				ok, err := repo.Condemn(ctx, tx.SqlTx(), ids[0], time.Now().UTC().Add(-200*time.Millisecond))
				require.NoError(t, err)
				require.True(t, ok)
				return nil
			}))
			require.NoError(t, fs.DeletePart(ctx, nil, ids[0]))
		})
	}
}
