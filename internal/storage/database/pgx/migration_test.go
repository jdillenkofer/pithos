package pgx

import (
	"context"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestMigrationsLeaveEveryPoolConnectionToTheStorage(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := t.Context()
	container, err := setupPostgresContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(context.Background())) })
	dbURL, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := OpenDatabase(dbURL, &ConnectionPoolConfiguration{MaxOpenConns: 1, MaxIdleConns: 1})
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		_, err := tx.SqlTx().ExecContext(ctx, "SELECT 1")
		return err
	}))
}
