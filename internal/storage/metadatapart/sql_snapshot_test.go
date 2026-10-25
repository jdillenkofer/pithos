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
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partcontent"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	"github.com/stretchr/testify/require"
)

type snapshotPhaseMetadataStore struct {
	metadatastore.MetadataStore
	betweenStatements func()
}

func (s *snapshotPhaseMetadataStore) HeadObject(ctx context.Context, tx *sql.Tx, bucket metadatastore.BucketName, key metadatastore.ObjectKey) (*metadatastore.Object, error) {
	// Pin the metadata snapshot, then let deletion commit before subsequent
	// metadata queries. A deadline with READ COMMITTED cannot make this safe.
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM objects").Scan(&count); err != nil {
		return nil, err
	}
	s.betweenStatements()
	return s.MetadataStore.HeadObject(ctx, tx, bucket, key)
}

func TestSQLGetStableMetadataAndLazyChunksAcrossGC(t *testing.T) {
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
			st.readerLifetime = time.Minute
			repo, err := repositoryfactory.NewPartContentRepository(st.db)
			require.NoError(t, err)
			registry, err := repositoryfactory.NewPartRegistryRepository(st.db)
			require.NoError(t, err)
			bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			var ids []partstore.PartId
			require.NoError(t, database.WithTx(ctx, st.db, nil, func(ctx context.Context, tx database.Tx) error {
				var parts []metadatastore.Part
				for range 2 {
					id, err := partstore.NewRandomPartId()
					require.NoError(t, err)
					ids = append(ids, *id)
					// Seed small chunks directly to exercise real lazy SQL reads without
					// allocating the production 256MB chunk size.
					for i, data := range []string{"abc", "def", "ghi"} {
						require.NoError(t, repo.SavePartContent(ctx, tx.SqlTx(), partcontent.DefaultPartStoreId, &partcontent.Entity{Id: id, ChunkIndex: i, Content: []byte(data)}))
					}
					parts = append(parts, metadatastore.Part{Id: *id, Size: 9, ETag: "etag"})
				}
				_, err := st.metadataStore.PutObject(ctx, tx.SqlTx(), bucket, &metadatastore.Object{Key: key, Size: 18, ETag: "etag", Parts: parts}, nil)
				return err
			}))
			original := st.metadataStore
			st.metadataStore = &snapshotPhaseMetadataStore{MetadataStore: original, betweenStatements: func() {
				// Avoid recursively using the metadata wrapper in the independent delete.
				st.metadataStore = original
				_, err := st.DeleteObject(ctx, bucket, key, nil)
				require.NoError(t, err)
			}}
			_, readers, err := st.GetObject(ctx, bucket, key, nil, nil)
			require.NoError(t, err, "metadata statements must share the pre-delete snapshot")
			defer readers[0].Close()
			_, bounded := readers[0].(interface{ Deadline() time.Time })
			require.False(t, bounded, "only a verified stable database snapshot may omit the bound")
			prefix := make([]byte, 2)
			_, err = io.ReadFull(readers[0], prefix)
			require.NoError(t, err)
			require.Equal(t, "ab", string(prefix))
			require.NoError(t, database.WithTx(ctx, st.db, nil, func(ctx context.Context, tx database.Tx) error {
				for _, id := range ids {
					condemned, err := registry.Condemn(ctx, tx.SqlTx(), id, time.Now().Add(-time.Hour))
					require.NoError(t, err)
					require.False(t, condemned)
				}
				return nil
			}))
			require.NoError(t, database.WithTx(ctx, st.db, nil, func(ctx context.Context, tx database.Tx) error {
				// Advance eligibility explicitly to prove lazy SQL protection even when
				// physical GC commits during a valid reader (not merely after expiry).
				_, err := tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET unreferenced_at=$1", time.Now().UTC().Add(-2*time.Hour))
				require.NoError(t, err)
				for _, id := range ids {
					condemned, err := registry.Condemn(ctx, tx.SqlTx(), id, time.Now().Add(-time.Hour))
					require.NoError(t, err)
					require.True(t, condemned)
					require.NoError(t, st.partStores.Default().DeletePart(ctx, tx, id))
				}
				return nil
			}))
			remaining, err := io.ReadAll(readers[0])
			require.NoError(t, err)
			require.Equal(t, "cdefghiabcdefghi", string(remaining))
			require.NoError(t, readers[0].Close())
			require.NoError(t, database.WithTx(ctx, st.db, nil, func(ctx context.Context, tx database.Tx) error {
				physical, err := st.partStores.Default().GetPartIds(ctx, tx)
				require.NoError(t, err)
				require.Empty(t, physical)
				return nil
			}))
		})
	}
}

func TestExternalGetStableMetadataPhase(t *testing.T) {
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
			defer fs.Stop(ctx)
			originalStores := st.partStores
			defer func() { st.partStores = originalStores }()
			st.partStores, err = partstore.NewNamedPartStores(fs, nil, nil)
			require.NoError(t, err)
			bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			_, err = st.PutObject(ctx, bucket, key, nil, ioutils.NewByteReadSeekCloser([]byte("content")), nil, nil)
			require.NoError(t, err)
			original := st.metadataStore
			st.metadataStore = &snapshotPhaseMetadataStore{MetadataStore: original, betweenStatements: func() {
				st.metadataStore = original
				_, err := st.DeleteObject(ctx, bucket, key, nil)
				require.NoError(t, err)
			}}
			_, readers, err := st.GetObject(ctx, bucket, key, nil, nil)
			require.NoError(t, err, "the short bounded metadata phase also needs a stable snapshot")
			defer readers[0].Close()
			_, bounded := readers[0].(interface{ Deadline() time.Time })
			require.True(t, bounded)
			data, err := io.ReadAll(readers[0])
			require.NoError(t, err)
			require.Equal(t, "content", string(data))
		})
	}
}

func TestSQLGetAmbientSnapshotValidation(t *testing.T) {
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
			bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			_, err := st.PutObject(ctx, bucket, key, nil, ioutils.NewByteReadSeekCloser([]byte("content")), nil, nil)
			require.NoError(t, err)
			for _, isolation := range []sql.IsolationLevel{sql.LevelDefault, sql.LevelRepeatableRead, sql.LevelSerializable} {
				tx, err := st.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: isolation})
				require.NoError(t, err)
				for _, ambient := range []*database.TxController{tx, tx.Child()} {
					_, readers, err := st.GetObject(database.ContextWithTx(ctx, ambient), bucket, key, nil, nil)
					if backend == "postgres" && isolation == sql.LevelDefault {
						// A reused default READ COMMITTED transaction cannot be upgraded by
						// GetObject's BeginTx options, including through a nested controller.
						require.ErrorContains(t, err, "requires a stable snapshot")
						require.Empty(t, readers)
					} else {
						require.NoError(t, err)
						data, err := io.ReadAll(readers[0])
						require.NoError(t, err)
						require.Equal(t, "content", string(data))
						require.NoError(t, readers[0].Close())
					}
				}
				require.NoError(t, tx.Rollback(ctx))
			}
		})
	}
}
