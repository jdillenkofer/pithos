package partregistry

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

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
		unreferenced, err := repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 2}})
		require.NoError(t, err)
		assert.Empty(t, unreferenced)

		// Removing the last reference reports the part as unreferenced.
		unreferenced, err = repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 1}})
		require.NoError(t, err)
		assert.Equal(t, []partstore.PartId{partId}, unreferenced)

		entities, err := repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		assert.Empty(t, entities)
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

		unreferenced, err := repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: condemnedPartId, Delta: 1}})
		require.NoError(t, err)
		assert.Equal(t, []partstore.PartId{condemnedPartId}, unreferenced)

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
		unreferenced, err := repo.RemoveReferences(ctx, tx, []partregistry.Ref{
			{PartId: missingPartId, Delta: 1},
			{PartId: registeredPartId, Delta: 1},
		})
		require.NoError(t, err)
		assert.Equal(t, []partstore.PartId{registeredPartId}, unreferenced)
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
		unreferenced, err := repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: partId, Delta: 2}})
		require.NoError(t, err)
		assert.Empty(t, unreferenced)

		entities, err := repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.Len(t, entities, 1)
		assert.Equal(t, int64(1), entities[0].RefCount)
		return nil
	})
}

func TestUpdateRefCountAndDeleteByPartId(t *testing.T) {
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

		changed, err = repo.DeleteByPartId(ctx, tx, partId, 2)
		require.NoError(t, err)
		require.True(t, changed)

		entities, err = repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		assert.Empty(t, entities)
		return nil
	})
}
