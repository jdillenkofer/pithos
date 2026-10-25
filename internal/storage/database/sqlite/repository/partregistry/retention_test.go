package partregistry

import (
	"context"
	"database/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/stretchr/testify/require"
)

func TestDereferenceRetentionAndRereference(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()
	id := mustNewPartId(t)
	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		require.NoError(t, repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: id, Delta: 1}}))
		_, err := tx.ExecContext(ctx, "UPDATE part_registry SET created_at = $1 WHERE part_id = $2", time.Now().Add(-365*24*time.Hour), id.String())
		require.NoError(t, err)
		removed, err := repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: id, Delta: 1}})
		require.NoError(t, err)
		require.Empty(t, removed)
		rows, err := repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Nil(t, rows[0].UnreferencedAt)
		ok, err := repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok) // First observation never condemns, even with a future cutoff.
		rows, err = repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.NotNil(t, rows[0].UnreferencedAt)
		dereferenced := *rows[0].UnreferencedAt
		// Old creation time cannot make a newly dereferenced part collectible.
		ok, err = repo.Condemn(ctx, tx, id, dereferenced.Add(-time.Nanosecond))
		require.NoError(t, err)
		require.False(t, ok)
		// Reconciliation of an already-zero count must not restart retention.
		ok, err = repo.UpdateRefCount(ctx, tx, id, 0, rows[0].Version)
		require.NoError(t, err)
		require.True(t, ok)
		rows, err = repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.True(t, dereferenced.Equal(*rows[0].UnreferencedAt))
		ok, err = repo.TryAddReferences(ctx, tx, []partregistry.Ref{{PartId: id, Delta: 1}})
		require.NoError(t, err)
		require.True(t, ok)
		rows, err = repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.Nil(t, rows[0].UnreferencedAt)
		ok, err = repo.Condemn(ctx, tx, id, dereferenced.Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok)
		_, err = repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: id, Delta: 1}})
		require.NoError(t, err)
		ok, err = repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok) // Re-reference reset the observation.
		ok, err = repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = repo.TryAddReferences(ctx, tx, []partregistry.Ref{{PartId: id, Delta: 1}})
		require.NoError(t, err)
		require.False(t, ok)
		return nil
	})
}

func TestOrphanedRowLifecycle(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()
	id := mustNewPartId(t)
	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		// A physical part without registry row or metadata becomes orphaned.
		ok, err := repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok)
		rows, err := repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.True(t, rows[0].Orphaned)
		require.Zero(t, rows[0].RefCount, "orphaned rows keep the reference count invariant")
		require.Nil(t, rows[0].UnreferencedAt)

		ok, err = repo.TryAddReferences(ctx, tx, []partregistry.Ref{{PartId: id, Delta: 1}})
		require.NoError(t, err)
		require.False(t, ok, "orphaned parts are never referenced")
		// Reconciling an unchanged zero count keeps the row orphaned.
		ok, err = repo.UpdateRefCount(ctx, tx, id, 0, rows[0].Version)
		require.NoError(t, err)
		require.True(t, ok)
		rows, err = repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.True(t, rows[0].Orphaned)

		ok, err = repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok, "orphaned rows need an observation first")
		ok, err = repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.True(t, ok)
		rows, err = repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.Empty(t, rows)
		return nil
	})
}

func TestReconciliationClearsOrphanedForReferencedPart(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()
	id := mustNewPartId(t)
	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		_, err := repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		rows, err := repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		ok, err := repo.UpdateRefCount(ctx, tx, id, 2, rows[0].Version)
		require.NoError(t, err)
		require.True(t, ok)
		rows, err = repo.FindAllEntities(ctx, tx)
		require.NoError(t, err)
		require.False(t, rows[0].Orphaned)
		require.EqualValues(t, 2, rows[0].RefCount)
		return nil
	})
}
