package partregistry

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRepository(t *testing.T) (database.Database, partregistry.Repository, func()) {
	t.Helper()
	storagePath, err := os.MkdirTemp("", "pithos-test-partregistry-")
	require.NoError(t, err)
	db, err := sqlite.OpenDatabase(filepath.Join(storagePath, "pithos.db"))
	require.NoError(t, err)
	repo, err := NewRepository()
	require.NoError(t, err)
	cleanup := func() {
		db.Close()
		os.RemoveAll(storagePath)
	}
	return db, repo, cleanup
}

func withWriteTx(t *testing.T, db database.Database, fn func(ctx context.Context, tx *sql.Tx) error) {
	t.Helper()
	err := database.WithTx(context.Background(), db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return fn(ctx, tx.SqlTx())
	})
	require.NoError(t, err)
}

func mustNewPartId(t *testing.T) partstore.PartId {
	t.Helper()
	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	return *partId
}

func TestRegisterAddRemoveLifecycle(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()

	partId := mustNewPartId(t)

	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		err := repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 1}})
		require.NoError(t, err)

		ok, err := repo.TryAddReferences(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 2}})
		require.NoError(t, err)
		assert.True(t, ok)

		// 3 references: removing 2 keeps the part alive.
		err = repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 2}})
		require.NoError(t, err)

		// Removing the last reference retains the part until GC.
		err = repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 1}})
		require.NoError(t, err)

		entities, err := repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.Len(t, entities, 1)
		assert.Zero(t, entities[0].RefCount)
		assert.Nil(t, entities[0].UnreferencedAt) // Only a collector observation starts retention.
		return nil
	})
}

func TestTryAddReferencesFailsForCondemnedPart(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()

	alivePartId := mustNewPartId(t)
	condemnedPartId := mustNewPartId(t)

	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		err := repo.RegisterParts(ctx, tx, []partregistry.Ref{
			{PartId: alivePartId, Delta: 1},
			{PartId: condemnedPartId, Delta: 1},
		})
		require.NoError(t, err)

		err = repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: condemnedPartId, Delta: 1}})
		require.NoError(t, err)
		condemned, err := repo.Condemn(ctx, tx, condemnedPartId, time.Now().UTC().Add(time.Hour))
		require.NoError(t, err)
		require.False(t, condemned) // First observe, then collect after retention.
		condemned, err = repo.Condemn(ctx, tx, condemnedPartId, time.Now().UTC().Add(time.Hour))
		require.NoError(t, err)
		require.True(t, condemned)

		// Adding a reference to a part whose registry row is gone must fail,
		// even when other refs in the same batch are addable.
		ok, err := repo.TryAddReferences(ctx, tx, []partregistry.Ref{
			{PartId: alivePartId, Delta: 1},
			{PartId: condemnedPartId, Delta: 1},
		})
		require.NoError(t, err)
		assert.False(t, ok)
		return nil
	})
}

func TestRemoveReferencesSkipsMissingRows(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()

	registeredPartId := mustNewPartId(t)
	missingPartId := mustNewPartId(t)

	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		err := repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: registeredPartId, Delta: 1}})
		require.NoError(t, err)

		// A missing row is skipped (leak-until-GC), not an error, and must not
		// prevent decrements of other refs in the batch.
		err = repo.RemoveReferences(ctx, tx, []partregistry.Ref{
			{PartId: missingPartId, Delta: 1},
			{PartId: registeredPartId, Delta: 1},
		})
		require.NoError(t, err)
		return nil
	})
}

func TestRemoveReferencesSkipsInsufficientRefCount(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()

	partId := mustNewPartId(t)

	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		err := repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 1}})
		require.NoError(t, err)

		// Decrementing by more than the current count is skipped entirely; the
		// row keeps its count so the part is never physically deleted.
		err = repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 2}})
		require.NoError(t, err)

		entities, err := repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.Len(t, entities, 1)
		assert.Equal(t, int64(1), entities[0].RefCount)
		return nil
	})
}

func TestUpdateRefCount(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()

	partId := mustNewPartId(t)

	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		err := repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 3}})
		require.NoError(t, err)

		changed, err := repo.UpdateRefCount(ctx, tx, partId, 1, 1)
		require.NoError(t, err)
		require.True(t, changed)

		entities, err := repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.Len(t, entities, 1)
		assert.Equal(t, int64(1), entities[0].RefCount)
		return nil
	})
}

func TestRegisterUnreferencedIsCondemnedByFirstSweep(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()

	partId := mustNewPartId(t)

	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		require.NoError(t, repo.RegisterUnreferenced(ctx, tx, partId, nil))
		// The part was never referenced, so no retention applies.
		condemned, err := repo.Condemn(ctx, tx, partId, time.Now().UTC().Add(-time.Hour))
		require.NoError(t, err)
		assert.True(t, condemned)
		return nil
	})
}

func TestRegisterUnreferencedKeepsExistingRow(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()

	referencedPartId := mustNewPartId(t)
	orphanPartId := mustNewPartId(t)

	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		require.NoError(t, repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: referencedPartId, Delta: 1}}))
		tracked, err := repo.TrackOrphan(ctx, tx, orphanPartId, nil)
		require.NoError(t, err)
		require.True(t, tracked)

		require.NoError(t, repo.RegisterUnreferenced(ctx, tx, referencedPartId, nil))
		require.NoError(t, repo.RegisterUnreferenced(ctx, tx, orphanPartId, nil))

		entities, err := repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.Len(t, entities, 2)
		for _, entity := range entities {
			assert.Nil(t, entity.UnreferencedAt)
			if entity.PartId == referencedPartId {
				assert.Equal(t, int64(1), entity.RefCount)
				assert.False(t, entity.Orphaned)
			} else {
				assert.False(t, entity.Orphaned)
			}
		}
		return nil
	})
}

func TestRegisterPartsAdoptsTrackedRow(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()

	partId := mustNewPartId(t)
	storeName := "cold"

	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		tracked, err := repo.TrackOrphan(ctx, tx, partId, nil)
		require.NoError(t, err)
		require.True(t, tracked)
		// The first sweep observation starts retention, but does not condemn.
		condemned, err := repo.Condemn(ctx, tx, partId, time.Now().UTC().Add(-time.Hour))
		require.NoError(t, err)
		require.False(t, condemned)

		require.NoError(t, repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 2, StoreName: &storeName}}))
		entities, err := repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.Len(t, entities, 1)
		assert.Equal(t, int64(2), entities[0].RefCount)
		assert.Nil(t, entities[0].UnreferencedAt)
		assert.False(t, entities[0].Orphaned)
		unreferenced, err := repo.FindUnreferenced(ctx, tx, time.Now().UTC().Add(time.Hour), nil, 10)
		require.NoError(t, err)
		assert.Empty(t, unreferenced)
		return nil
	})
}

func TestRegisterPartsFailsForCondemnedPart(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()

	condemnedPartId := mustNewPartId(t)
	referencedPartId := mustNewPartId(t)

	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		require.NoError(t, repo.RegisterUnreferenced(ctx, tx, condemnedPartId, nil))
		condemned, err := repo.Condemn(ctx, tx, condemnedPartId, time.Now().UTC().Add(-time.Hour))
		require.NoError(t, err)
		require.True(t, condemned)
		require.NoError(t, repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: referencedPartId, Delta: 1}}))

		// The part may already be deleted, so the write must not reference it.
		err = repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: condemnedPartId, Delta: 1}})
		require.ErrorIs(t, err, partregistry.ErrPartCondemned)
		// A live row is never overwritten by a colliding registration.
		err = repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: referencedPartId, Delta: 1}})
		require.ErrorIs(t, err, partregistry.ErrPartCondemned)
		return nil
	})
}
