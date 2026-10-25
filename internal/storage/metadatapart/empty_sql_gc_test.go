package metadatapart

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/gc"
	"github.com/stretchr/testify/require"
)

func TestSQLEmptyObjectRepeatedDeleteGC(t *testing.T) {
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
			registry, err := repositoryfactory.NewPartRegistryRepository(st.db)
			require.NoError(t, err)
			dedup, err := repositoryfactory.NewPartDedupIndexRepository(st.db)
			require.NoError(t, err)
			collector, err := gc.New(st.db, st.metadataStore, st.partStores, registry, dedup, 100*time.Millisecond, 5*time.Millisecond)
			require.NoError(t, err)
			rows := func() []partregistry.Entity {
				t.Helper()
				var entities []partregistry.Entity
				require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
					var err error
					entities, err = registry.FindAllEntities(ctx, tx.SqlTx())
					return err
				}))
				return entities
			}
			bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("empty")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			for range 3 {
				_, err := st.PutObject(ctx, bucket, key, nil, ioutils.NewByteReadSeekCloser(nil), nil, nil)
				require.NoError(t, err)
				require.Len(t, rows(), 1)
				require.EqualValues(t, 1, rows()[0].RefCount)
				require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
					physical, err := st.partStores.Default().GetPartIds(ctx, tx)
					require.NoError(t, err)
					require.Empty(t, physical, "plain SQL stores empty parts without content rows")
					return nil
				}))
				_, err = st.DeleteObject(ctx, bucket, key, nil)
				require.NoError(t, err)
				deletedAt := time.Now()
				zero := rows()
				require.Len(t, zero, 1)
				require.Zero(t, zero[0].RefCount)
				require.Nil(t, zero[0].UnreferencedAt, "deletion must not start the GC observation clock")
				var stop atomic.Bool
				done := make(chan struct{})
				go func() { defer close(done); collector.RunGCLoop(&stop) }()
				defer func() { stop.Store(true); <-done }()
				require.Eventually(t, func() bool {
					current := rows()
					return len(current) == 1 && current[0].UnreferencedAt != nil
				}, 3*time.Second, time.Millisecond)
				observed := rows()
				require.Len(t, observed, 1)
				require.False(t, observed[0].UnreferencedAt.Before(deletedAt))
				require.Eventually(t, func() bool { return len(rows()) == 0 }, 3*time.Second, 5*time.Millisecond)
				stop.Store(true)
				<-done
			}
		})
	}
}
