package gc

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/readerlease"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type failingCleanupLeaseStore struct {
	readerlease.Store
}

func (failingCleanupLeaseStore) DeleteExpired(context.Context) (int64, error) {
	return 0, errors.New("lease table unavailable")
}

func registryRowCount(t *testing.T, env *gcTestEnv) int {
	t.Helper()
	var count int
	require.NoError(t, database.WithTx(context.Background(), env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rows, err := env.registryRepo.FindAllEntities(ctx, tx.SqlTx())
		count = len(rows)
		return err
	}))
	return count
}

func TestSweepSkipsLeasedPartsAndSurvivesLeaseCleanupFailure(t *testing.T) {
	testutils.SkipIfIntegration(t)
	forEachGCBackend(t, func(t *testing.T, env *gcTestEnv) {
		ctx := context.Background()
		leases := readerlease.NewMemoryStore(time.Minute)
		env.collector.readerLeases = failingCleanupLeaseStore{Store: leases}
		leasedId, freeId := oldPartId(), oldPartId()
		require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
			return env.registryRepo.RegisterParts(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: leasedId, Delta: 0}, {PartId: freeId, Delta: 0}})
		}))
		require.NoError(t, leases.Acquire(ctx, "lease", []partstore.PartId{leasedId}))

		require.NoError(t, env.collector.runSweepWithContext(ctx))
		ageUnreferenced(t, env)
		require.NoError(t, env.collector.runSweepWithContext(ctx))
		require.Equal(t, 1, registryRowCount(t, env), "only the leased part survives")

		// Retention of the leased part only starts once its lease is gone.
		require.NoError(t, leases.Release(ctx, []string{"lease"}))
		require.NoError(t, env.collector.runSweepWithContext(ctx))
		ageUnreferenced(t, env)
		require.NoError(t, env.collector.runSweepWithContext(ctx))
		require.Zero(t, registryRowCount(t, env))
	})
}
