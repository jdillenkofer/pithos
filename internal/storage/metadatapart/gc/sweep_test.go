package gc

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

type listingCountingStore struct {
	partstore.PartStore
	listings int
}

func (s *listingCountingStore) Capabilities() partstore.Capabilities {
	return partstore.CapabilitiesOf(s.PartStore)
}

func (s *listingCountingStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	s.listings++
	return s.PartStore.GetPartIds(ctx, tx)
}

func forEachGCBackend(t *testing.T, fn func(t *testing.T, env *gcTestEnv)) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			if backend == "postgres" {
				fn(t, newPostgresGCTestEnv(t))
			} else {
				fn(t, newSqliteGCTestEnv(t))
			}
		})
	}
}

func oldPartId() partstore.PartId {
	return *partstore.MustNewPartIdFromString(ulid.MustNew(ulid.Timestamp(time.Now().Add(-24*time.Hour)), rand.Reader).String())
}

func ageUnreferenced(t *testing.T, env *gcTestEnv) {
	t.Helper()
	require.NoError(t, database.WithTx(context.Background(), env.db, nil, func(ctx context.Context, tx database.Tx) error {
		_, err := tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET unreferenced_at = $1 WHERE unreferenced_at IS NOT NULL", time.Now().UTC().Add(-time.Hour))
		return err
	}))
}

func TestSweepReclaimsFromRecordedStoreWithoutListing(t *testing.T) {
	testutils.SkipIfIntegration(t)
	forEachGCBackend(t, func(t *testing.T, env *gcTestEnv) {
		ctx := context.Background()
		fs, err := filesystem.New(t.TempDir())
		require.NoError(t, err)
		require.NoError(t, fs.Start(ctx))
		t.Cleanup(func() { require.NoError(t, fs.Stop(ctx)) })
		defaultStore := &listingCountingStore{PartStore: env.physicalStore}
		cold := &listingCountingStore{PartStore: fs}
		env.collector.partStores, err = partstore.NewNamedPartStores(defaultStore, map[string]partstore.PartStore{"cold": cold}, nil)
		require.NoError(t, err)
		coldName := "cold"
		id := oldPartId()
		require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
			require.NoError(t, fs.PutPart(ctx, tx, id, bytes.NewBufferString("cold data")))
			require.NoError(t, env.registryRepo.RegisterParts(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: id, Delta: 1, StoreName: &coldName}}))
			_, err := env.registryRepo.RemoveReferences(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: id, Delta: 1}})
			return err
		}))
		coldIds := func() []partstore.PartId {
			var ids []partstore.PartId
			require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
				var err error
				ids, err = fs.GetPartIds(ctx, tx)
				return err
			}))
			return ids
		}

		require.NoError(t, env.collector.runSweepWithContext(ctx))
		require.Contains(t, coldIds(), id, "the first sweep only starts retention")
		ageUnreferenced(t, env)
		require.NoError(t, env.collector.runSweepWithContext(ctx))
		require.NotContains(t, coldIds(), id)
		require.Zero(t, defaultStore.listings)
		require.Zero(t, cold.listings)
		require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			rows, err := env.registryRepo.FindAllEntities(ctx, tx.SqlTx())
			require.Empty(t, rows)
			return err
		}))
	})
}

func TestSweepPagesPastRowsItCannotCondemn(t *testing.T) {
	testutils.SkipIfIntegration(t)
	forEachGCBackend(t, func(t *testing.T, env *gcTestEnv) {
		ctx := context.Background()
		refs := make([]partregistry.Ref, 0, 2*gcBatchSize+1)
		for range cap(refs) {
			refs = append(refs, partregistry.Ref{PartId: oldPartId(), Delta: 0})
		}
		require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
			return env.registryRepo.RegisterParts(ctx, tx.SqlTx(), refs)
		}))
		require.NoError(t, env.collector.runSweepWithContext(ctx))
		require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			rows, err := env.registryRepo.FindAllEntities(ctx, tx.SqlTx())
			require.Len(t, rows, len(refs))
			for _, row := range rows {
				require.NotNil(t, row.UnreferencedAt, "every page is observed")
			}
			return err
		}))
		ageUnreferenced(t, env)
		require.NoError(t, env.collector.runSweepWithContext(ctx))
		require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			rows, err := env.registryRepo.FindAllEntities(ctx, tx.SqlTx())
			require.Empty(t, rows)
			return err
		}))
	})
}

func TestFullPassTracksUntrackedPartsWithoutDeleting(t *testing.T) {
	testutils.SkipIfIntegration(t)
	forEachGCBackend(t, func(t *testing.T, env *gcTestEnv) {
		ctx := context.Background()
		fs, err := filesystem.New(t.TempDir())
		require.NoError(t, err)
		require.NoError(t, fs.Start(ctx))
		t.Cleanup(func() { require.NoError(t, fs.Stop(ctx)) })
		env.collector.partStores, err = partstore.NewNamedPartStores(env.physicalStore, map[string]partstore.PartStore{"cold": fs}, nil)
		require.NoError(t, err)
		old, young := oldPartId(), *partstore.MustNewPartIdFromString(ulid.Make().String())
		require.NoError(t, fs.PutPart(ctx, nil, old, bytes.NewBufferString("leftover")))
		require.NoError(t, fs.PutPart(ctx, nil, young, bytes.NewBufferString("in flight")))

		require.NoError(t, env.collector.runFullPassWithContext(ctx))
		require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			rows, err := env.registryRepo.FindAllEntities(ctx, tx.SqlTx())
			require.NoError(t, err)
			require.Len(t, rows, 1, "young parts may belong to an uncommitted write")
			require.Equal(t, old, rows[0].PartId)
			require.True(t, rows[0].Orphaned)
			ids, err := fs.GetPartIds(ctx, tx)
			require.ElementsMatch(t, []partstore.PartId{old, young}, ids)
			return err
		}))
		// The sweep reclaims the orphan from the store the full pass found it in.
		require.NoError(t, env.collector.runSweepWithContext(ctx))
		ageUnreferenced(t, env)
		require.NoError(t, env.collector.runSweepWithContext(ctx))
		require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			ids, err := fs.GetPartIds(ctx, tx)
			require.Equal(t, []partstore.PartId{young}, ids)
			return err
		}))
	})
}

func TestMetadataWritesRecordPartStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	forEachGCBackend(t, func(t *testing.T, env *gcTestEnv) {
		ctx := context.Background()
		coldName := "cold"
		hot, cold := oldPartId(), oldPartId()
		bucket, key := metadatastore.MustNewBucketName("bucket"), metadatastore.MustNewObjectKey("key")
		require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
			require.NoError(t, env.metadataStore.CreateBucket(ctx, tx.SqlTx(), bucket))
			_, err := env.metadataStore.PutObject(ctx, tx.SqlTx(), bucket, &metadatastore.Object{Key: key, Size: 2, Parts: []metadatastore.Part{{Id: hot, Size: 1}, {Id: cold, Size: 1, StoreName: &coldName}}}, nil)
			require.NoError(t, err)
			_, err = env.metadataStore.DeleteObject(ctx, tx.SqlTx(), bucket, key, nil)
			return err
		}))
		require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			unreferenced, err := env.registryRepo.FindUnreferenced(ctx, tx.SqlTx(), time.Now(), nil, gcBatchSize)
			require.NoError(t, err)
			stores := map[partstore.PartId]*string{}
			for _, u := range unreferenced {
				stores[u.PartId] = u.StoreName
			}
			require.Len(t, stores, 2)
			require.Nil(t, stores[hot])
			require.Equal(t, &coldName, stores[cold])
			return nil
		}))
	})
}

func TestFullPassRestoresMissingRowWithPartStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	forEachGCBackend(t, func(t *testing.T, env *gcTestEnv) {
		ctx := context.Background()
		coldName := "cold"
		id := oldPartId()
		bucket, key := metadatastore.MustNewBucketName("bucket"), metadatastore.MustNewObjectKey("key")
		require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
			require.NoError(t, env.metadataStore.CreateBucket(ctx, tx.SqlTx(), bucket))
			_, err := env.metadataStore.PutObject(ctx, tx.SqlTx(), bucket, &metadatastore.Object{Key: key, Size: 1, Parts: []metadatastore.Part{{Id: id, Size: 1, StoreName: &coldName}}}, nil)
			require.NoError(t, err)
			_, err = tx.SqlTx().ExecContext(ctx, "DELETE FROM part_registry WHERE part_id = $1", id.String())
			return err
		}))

		require.NoError(t, env.collector.runFullPassWithContext(ctx))
		require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			var storeName sql.NullString
			var refCount int64
			require.NoError(t, tx.SqlTx().QueryRowContext(ctx, "SELECT ref_count, part_store_name FROM part_registry WHERE part_id = $1", id.String()).Scan(&refCount, &storeName))
			require.EqualValues(t, 1, refCount)
			require.Equal(t, sql.NullString{String: coldName, Valid: true}, storeName)
			return nil
		}))
	})
}
