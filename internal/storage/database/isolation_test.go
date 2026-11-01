package database_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestNestedSQLiteTransactionCannotChooseIsolation(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db := openTestDB(t)
	err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		return database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}, func(ctx context.Context, tx database.Tx) error { return nil })
	})
	require.ErrorIs(t, err, database.ErrNestedIsolation)
}
