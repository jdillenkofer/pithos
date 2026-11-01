package readerlease

import (
	"context"
	"database/sql"
	"testing"
	"time"

	pgxdatabase "github.com/jdillenkofer/pithos/internal/storage/database/pgx"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const testTTL = 400 * time.Millisecond

func newPartIds(t *testing.T, n int) []partstore.PartId {
	t.Helper()
	ids := make([]partstore.PartId, n)
	for i := range ids {
		id, err := partstore.NewRandomPartId()
		require.NoError(t, err)
		ids[i] = *id
	}
	return ids
}

func requireLeased(t *testing.T, store Store, partIds []partstore.PartId, expected ...partstore.PartId) {
	t.Helper()
	leased, err := store.Leased(context.Background(), nil, partIds)
	require.NoError(t, err)
	require.Len(t, leased, len(expected))
	for _, id := range expected {
		require.Contains(t, leased, id)
	}
}

// runStoreScenarios checks a store returned by newStore.
func runStoreScenarios(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("acquire and release", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		ids := newPartIds(t, 3)
		require.NoError(t, store.Acquire(ctx, "lease-a", ids[:2]))
		requireLeased(t, store, ids, ids[0], ids[1])
		require.NoError(t, store.Release(ctx, []string{"lease-a"}))
		requireLeased(t, store, ids)
	})

	t.Run("expired leases protect nothing and are deleted", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		ids := newPartIds(t, 1)
		require.NoError(t, store.Acquire(ctx, "lease-b", ids))
		time.Sleep(testTTL + 100*time.Millisecond)
		requireLeased(t, store, ids)
		// Renewal never revives a lease GC may already have ignored.
		require.NoError(t, store.Renew(ctx, []string{"lease-b"}))
		requireLeased(t, store, ids)
		deleted, err := store.DeleteExpired(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, deleted, int64(1))
	})

	t.Run("renewal extends unexpired leases", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		ids := newPartIds(t, 1)
		require.NoError(t, store.Acquire(ctx, "lease-c", ids))
		for range 4 {
			time.Sleep(testTTL / 2)
			require.NoError(t, store.Renew(ctx, []string{"lease-c"}))
		}
		requireLeased(t, store, ids, ids[0])
		require.NoError(t, store.Release(ctx, []string{"lease-c"}))
	})

	t.Run("renewal extends only the given leases", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		ids := newPartIds(t, 2)
		require.NoError(t, store.Acquire(ctx, "lease-d", ids[:1]))
		require.NoError(t, store.Acquire(ctx, "lease-e", ids[1:]))
		for range 4 {
			time.Sleep(testTTL / 2)
			require.NoError(t, store.Renew(ctx, []string{"lease-e"}))
		}
		// A lease nobody tracks any more, such as one whose release
		// failed, expires although its owner renews others.
		requireLeased(t, store, ids, ids[1])
		require.NoError(t, store.Release(ctx, []string{"lease-e"}))
	})

	t.Run("acquire is idempotent", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		ids := newPartIds(t, 2)
		require.NoError(t, store.Acquire(ctx, "lease-f", ids))
		require.NoError(t, store.Acquire(ctx, "lease-f", ids))
		requireLeased(t, store, ids, ids...)
		require.NoError(t, store.Release(ctx, []string{"lease-f"}))
		requireLeased(t, store, ids)
	})
}

func TestMemoryStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	runStoreScenarios(t, func(t *testing.T) Store { return NewMemoryStore(testTTL) })
}

func TestPostgresStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	container, err := testutils.RunContainer(func() (*postgres.PostgresContainer, error) {
		return postgres.Run(ctx, testutils.PostgresImage,
			postgres.WithUsername("postgres"), postgres.WithPassword("postgres"), postgres.WithDatabase("postgres"), postgres.BasicWaitStrategies())
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })
	dbURL, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db, err := pgxdatabase.OpenDatabase(dbURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	newStore := func(t *testing.T) Store {
		store, err := New(db, testTTL)
		require.NoError(t, err)
		return store
	}
	runStoreScenarios(t, newStore)

	t.Run("lease renewed after the snapshot of the lookup stays leased", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		ids := newPartIds(t, 1)
		require.NoError(t, store.Acquire(ctx, "lease-g", ids))
		// The lookup's snapshot predates a renewal that commits in time; the
		// lookup runs only after the expiry that snapshot still shows.
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		require.NoError(t, err)
		defer func() { require.NoError(t, tx.Rollback(ctx)) }()
		_, err = tx.SqlTx().ExecContext(ctx, "SELECT 1")
		require.NoError(t, err)
		time.Sleep(testTTL / 2)
		require.NoError(t, store.Renew(ctx, []string{"lease-g"}))
		time.Sleep(testTTL/2 + 100*time.Millisecond)
		leased, err := store.Leased(ctx, tx, ids)
		require.NoError(t, err)
		require.Contains(t, leased, ids[0])
		require.NoError(t, store.Release(ctx, []string{"lease-g"}))
	})
}
