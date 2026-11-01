package pgx

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestWithTxRetryRunsSerializationFailuresAgain(t *testing.T) {
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
	db, err := OpenDatabase(dbURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.ExecContext(ctx, "CREATE TABLE counter (id INT PRIMARY KEY, value INT NOT NULL)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO counter VALUES (1, 0)")
	require.NoError(t, err)

	attempts := 0
	err = database.WithTxRetry(ctx, db, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(ctx context.Context, tx database.Tx) error {
		attempts++
		var value int
		if err := tx.SqlTx().QueryRowContext(ctx, "SELECT value FROM counter WHERE id = 1").Scan(&value); err != nil {
			return err
		}
		if attempts == 1 {
			// A concurrent writer commits after this transaction's snapshot.
			if _, err := db.ExecContext(ctx, "UPDATE counter SET value = value + 1 WHERE id = 1"); err != nil {
				return err
			}
		}
		_, err := tx.SqlTx().ExecContext(ctx, "UPDATE counter SET value = $1 WHERE id = 1", value+1)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	var value int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT value FROM counter WHERE id = 1").Scan(&value))
	require.Equal(t, 2, value)
}
