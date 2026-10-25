package partregistry

import (
	"context"
	"database/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/stretchr/testify/require"
)

func TestReaderLeaseBlocksCondemnUntilExpiry(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()
	id := mustNewPartId(t)
	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		require.NoError(t, repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: id, Delta: 1}}))
		_, err := repo.RemoveReferences(ctx, tx, []partregistry.Ref{{PartId: id, Delta: 1}})
		require.NoError(t, err)
		ok, err := repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok) // First observation.

		now := time.Now().UTC()
		ok, err = repo.AcquireReaderLease(ctx, tx, "lease", []partstore.PartId{id, id}, now.Add(time.Hour))
		require.NoError(t, err)
		require.True(t, ok, "an observed but uncondemned part is still leasable")
		ok, err = repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok, "an active lease blocks condemnation")

		ok, err = repo.RenewReaderLease(ctx, tx, "lease", now, now.Add(-time.Second))
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = repo.RenewReaderLease(ctx, tx, "lease", now, now.Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok, "an expired lease cannot be renewed")
		ok, err = repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.True(t, ok, "an expired lease no longer protects the part")

		ok, err = repo.AcquireReaderLease(ctx, tx, "late", []partstore.PartId{id}, now.Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok, "condemned parts are never leased")
		var leases int
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM part_reader_lease WHERE id = $1", "late").Scan(&leases))
		require.Zero(t, leases)

		deleted, err := repo.DeleteExpiredReaderLeases(ctx, tx, time.Now().UTC())
		require.NoError(t, err)
		require.EqualValues(t, 1, deleted)
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM part_reader_lease_part").Scan(&leases))
		require.Zero(t, leases, "lease parts cascade with their lease")
		return nil
	})
}

func TestReaderLeaseRejectsOrphanedPart(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()
	id := mustNewPartId(t)
	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		ok, err := repo.Condemn(ctx, tx, id, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok)
		ok, err = repo.AcquireReaderLease(ctx, tx, "lease", []partstore.PartId{id}, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok)
		return nil
	})
}

func TestReleaseReaderLease(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, repo, cleanup := setupRepository(t)
	defer cleanup()
	id := mustNewPartId(t)
	withWriteTx(t, db, func(ctx context.Context, tx *sql.Tx) error {
		require.NoError(t, repo.RegisterParts(ctx, tx, []partregistry.Ref{{PartId: id, Delta: 1}}))
		ok, err := repo.AcquireReaderLease(ctx, tx, "lease", []partstore.PartId{id}, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, repo.ReleaseReaderLease(ctx, tx, "lease"))
		var parts int
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM part_reader_lease_part").Scan(&parts))
		require.Zero(t, parts)
		return nil
	})
}
