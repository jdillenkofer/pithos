package gc

import (
	"bytes"
	"context"
	"crypto/rand"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestRegistryOnlyCleanupPreservesNamedStoreRetention(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	env := newSqliteGCTestEnv(t)
	fs, err := filesystem.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, fs.Start(ctx))
	defer fs.Stop(ctx)
	external := &retryDeletionStore{PartStore: fs, fail: true}
	env.collector.partStores, err = partstore.NewNamedPartStores(env.physicalStore, map[string]partstore.PartStore{"cold": external}, nil)
	require.NoError(t, err)
	id := *partstore.MustNewPartIdFromString(ulid.MustNew(ulid.Timestamp(time.Now().Add(-24*time.Hour)), rand.Reader).String())
	require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
		// The default SQL namespace has no physical ID, but the named external
		// store has the same logical ID. Registry-only cleanup must not own it.
		require.NoError(t, env.physicalStore.PutPart(ctx, tx, id, bytes.NewReader(nil)))
		require.NoError(t, fs.PutPart(ctx, tx, id, bytes.NewBufferString("external")))
		return env.registryRepo.RegisterParts(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: id, Delta: 0}})
	}))
	assertState := func(wantRows int, observed bool, present bool) {
		t.Helper()
		require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
			rows, err := env.registryRepo.FindAllEntities(ctx, tx.SqlTx())
			require.NoError(t, err)
			require.Len(t, rows, wantRows)
			if wantRows > 0 {
				require.Equal(t, observed, rows[0].UnreferencedAt != nil)
			}
			ids, err := fs.GetPartIds(ctx, tx)
			require.NoError(t, err)
			require.Equal(t, present, len(ids) != 0)
			return nil
		}))
	}
	require.NoError(t, env.collector.runGC())
	assertState(1, true, true)
	require.NoError(t, env.collector.runGC())
	assertState(1, true, true)
	age := func() {
		require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
			_, err := tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET unreferenced_at=$1 WHERE part_id=$2", time.Now().UTC().Add(-time.Hour), id.String())
			return err
		}))
	}
	age()
	require.NoError(t, env.collector.runGC())
	// The failed post-commit delete must not be mistaken for an absent ID and
	// re-observed by registry-only cleanup in the same collection.
	assertState(0, false, true)
	require.NoError(t, env.collector.runGC())
	assertState(1, false, true)
	require.NoError(t, env.collector.runGC())
	assertState(1, true, true)
	age()
	require.NoError(t, env.collector.runGC())
	assertState(0, false, false)
}

func TestRegistryOnlyCleanupPreservesReReferences(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	env := newSqliteGCTestEnv(t)
	id := *partstore.MustNewPartIdFromString(ulid.MustNew(ulid.Timestamp(time.Now().Add(-24*time.Hour)), rand.Reader).String())
	bucket, key := metadatastore.MustNewBucketName("bucket"), metadatastore.MustNewObjectKey("empty")
	require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
		require.NoError(t, env.metadataStore.CreateBucket(ctx, tx.SqlTx(), bucket))
		return env.registryRepo.RegisterParts(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: id, Delta: 0}})
	}))
	require.NoError(t, env.collector.runGC())
	require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
		// Age the original observation, then re-reference before condemnation.
		_, err := tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET unreferenced_at=$1 WHERE part_id=$2", time.Now().UTC().Add(-time.Hour), id.String())
		require.NoError(t, err)
		ok, err := env.registryRepo.TryAddReferences(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: id, Delta: 1}})
		require.NoError(t, err)
		require.True(t, ok)
		_, err = env.metadataStore.PutObject(ctx, tx.SqlTx(), bucket, &metadatastore.Object{Key: key, Parts: []metadatastore.Part{{Id: id, RefPreAcquired: true}}}, nil)
		return err
	}))
	require.NoError(t, env.collector.runGC())
	require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
		rows, err := env.registryRepo.FindAllEntities(ctx, tx.SqlTx())
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 1, rows[0].RefCount)
		require.Nil(t, rows[0].UnreferencedAt)
		_, err = env.metadataStore.DeleteObject(ctx, tx.SqlTx(), bucket, key, nil)
		return err
	}))
	require.NoError(t, env.collector.runGC())
	require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
		rows, err := env.registryRepo.FindAllEntities(ctx, tx.SqlTx())
		require.NoError(t, err)
		require.Len(t, rows, 1, "the aged pre-re-reference observation must not authorize cleanup")
		require.Zero(t, rows[0].RefCount)
		require.NotNil(t, rows[0].UnreferencedAt)
		require.WithinDuration(t, time.Now(), *rows[0].UnreferencedAt, time.Second)
		return nil
	}))
}
