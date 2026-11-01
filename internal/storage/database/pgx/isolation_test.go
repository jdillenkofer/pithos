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

func openTestDatabase(t *testing.T) (*pgxDatabase, string) {
	t.Helper()
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
	return db, dbURL
}

func transactionIsolation(t *testing.T, ctx context.Context, db database.Database, opts *sql.TxOptions) (sql.IsolationLevel, string) {
	t.Helper()
	var level sql.IsolationLevel
	var shown string
	require.NoError(t, database.WithTx(ctx, db, opts, func(ctx context.Context, tx database.Tx) error {
		level = tx.(*database.TxController).Isolation()
		return tx.SqlTx().QueryRowContext(ctx, "SHOW transaction_isolation").Scan(&shown)
	}))
	return level, shown
}

func TestTransactionsUseServerDefaultIsolationUnlessTheyAsk(t *testing.T) {
	db, dbURL := openTestDatabase(t)
	ctx := t.Context()
	level, shown := transactionIsolation(t, ctx, db, nil)
	require.Equal(t, sql.LevelReadCommitted, level)
	require.Equal(t, "read committed", shown)

	_, err := db.ExecContext(ctx, "ALTER DATABASE postgres SET default_transaction_isolation = 'serializable'")
	require.NoError(t, err)
	serializableDb, err := OpenDatabase(dbURL, nil)
	require.NoError(t, err)
	defer serializableDb.Close()
	level, shown = transactionIsolation(t, ctx, serializableDb, nil)
	require.Equal(t, sql.LevelSerializable, level)
	require.Equal(t, "serializable", shown)
	level, shown = transactionIsolation(t, ctx, serializableDb, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.Equal(t, sql.LevelReadCommitted, level)
	require.Equal(t, "read committed", shown)
}

func TestNestedTransactionCannotAskForStricterIsolation(t *testing.T) {
	db, _ := openTestDatabase(t)
	ctx := t.Context()
	err := database.WithTx(ctx, db, &sql.TxOptions{Isolation: sql.LevelRepeatableRead}, func(ctx context.Context, tx database.Tx) error {
		require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, func(ctx context.Context, tx database.Tx) error { return nil }))
		require.NoError(t, database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error { return nil }))
		return database.WithTx(ctx, db, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(ctx context.Context, tx database.Tx) error { return nil })
	})
	require.ErrorIs(t, err, database.ErrIsolationTooWeak)
}
