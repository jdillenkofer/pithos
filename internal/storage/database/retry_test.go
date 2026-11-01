package database_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

var errConflict = errors.New("conflict")

type conflictingDatabase struct {
	database.Database
}

func (db *conflictingDatabase) IsRetryableTxError(err error) bool {
	return errors.Is(err, errConflict)
}

func (db *conflictingDatabase) UnwrapDatabase() database.Database {
	return db.Database
}

func TestWithTxRetryRetriesConflictsOnly(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db := &conflictingDatabase{Database: openTestDB(t)}
	attempts := 0
	require.NoError(t, database.WithTxRetry(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		attempts++
		if attempts < 3 {
			return errConflict
		}
		return nil
	}))
	require.Equal(t, 3, attempts)

	attempts = 0
	other := errors.New("other")
	require.ErrorIs(t, database.WithTxRetry(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		attempts++
		return other
	}), other)
	require.Equal(t, 1, attempts)

	attempts = 0
	require.ErrorIs(t, database.WithTxRetry(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		attempts++
		return errConflict
	}), errConflict)
	require.Equal(t, 5, attempts)
}

func TestWithTxRetryDoesNotRetryCommittedTransactions(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db := &conflictingDatabase{Database: openTestDB(t)}
	attempts := 0
	err := database.WithTxRetry(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		attempts++
		tx.OnAfterCommit(func(context.Context) error { return errConflict })
		return nil
	})
	require.ErrorIs(t, err, errConflict)
	require.Equal(t, 1, attempts)
}

func TestWithTxRetryInsideTransactionRunsOnce(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db := &conflictingDatabase{Database: openTestDB(t)}
	attempts := 0
	err := database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		return database.WithTxRetry(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
			attempts++
			return errConflict
		})
	})
	require.ErrorIs(t, err, errConflict)
	require.Equal(t, 1, attempts)
}

func TestNestedSQLiteTransactionAcceptsAnyIsolation(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db := openTestDB(t)
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		return database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}, func(ctx context.Context, tx database.Tx) error { return nil })
	}))
}
