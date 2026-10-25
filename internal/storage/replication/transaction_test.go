package replication

import (
	"context"
	"database/sql"
	"errors"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqlitedb "github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/stretchr/testify/require"
)

func TestRetryTransaction(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, state := range []string{"40001", "40P01"} {
		t.Run(state, func(t *testing.T) {
			db, err := sqlitedb.OpenDatabase(t.TempDir() + "/retry.db")
			require.NoError(t, err)
			defer db.Close()
			require.NoError(t, database.WithTx(t.Context(), db, nil, func(ctx context.Context, tx database.Tx) error {
				_, err := tx.SqlTx().ExecContext(ctx, "CREATE TABLE retry_test (value INTEGER)")
				return err
			}))
			attempts, hooks := 0, 0
			err = retryTransaction(t.Context(), db, nil, func() bool { return true }, func(ctx context.Context, tx database.Tx) error {
				attempts++
				tx.OnAfterCommit(func(context.Context) error { hooks++; return nil })
				_, err := tx.SqlTx().ExecContext(ctx, "INSERT INTO retry_test VALUES (1)")
				if err != nil {
					return err
				}
				if attempts < 3 {
					return &pgconn.PgError{Code: state}
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 3, attempts)
			require.Equal(t, 1, hooks)
			require.NoError(t, database.WithTx(t.Context(), db, nil, func(ctx context.Context, tx database.Tx) error {
				var count int
				require.NoError(t, tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM retry_test").Scan(&count))
				require.Equal(t, 1, count)
				return nil
			}))
		})
	}
}

func TestRetryTransactionStops(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, err := sqlitedb.OpenDatabase(t.TempDir() + "/retry.db")
	require.NoError(t, err)
	defer db.Close()
	conflict := &pgconn.PgError{Code: "40001"}
	for _, tc := range []struct {
		name    string
		safe    bool
		failure error
		want    int
	}{
		{"bounded", true, conflict, 3},
		{"external effect", false, conflict, 1},
		{"unrelated state", true, &pgconn.PgError{Code: "23505"}, 1},
		{"indeterminate", true, errors.New("connection lost"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			err := retryTransaction(t.Context(), db, nil, func() bool { return tc.safe }, func(context.Context, database.Tx) error { attempts++; return tc.failure })
			require.ErrorIs(t, err, tc.failure)
			require.Equal(t, tc.want, attempts)
		})
	}
	t.Run("postcommit failure is never replayed", func(t *testing.T) {
		attempts := 0
		err := retryTransaction(t.Context(), db, nil, func() bool { return true }, func(ctx context.Context, tx database.Tx) error {
			attempts++
			tx.OnAfterCommit(func(context.Context) error { return conflict })
			return nil
		})
		require.ErrorIs(t, err, conflict)
		require.Equal(t, 1, attempts)
	})
	t.Run("ambient ownership", func(t *testing.T) {
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		defer tx.Rollback(t.Context())
		err = retryTransaction(database.ContextWithTx(t.Context(), tx), db, nil, func() bool { return true }, func(context.Context, database.Tx) error { t.Fatal("must not run"); return nil })
		require.ErrorContains(t, err, "own transaction")
	})
	t.Run("cancellation during callback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		attempts := 0
		err := retryTransaction(ctx, db, nil, func() bool { return true }, func(context.Context, database.Tx) error {
			attempts++
			cancel()
			return conflict
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, attempts)
	})
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		attempts := 0
		err := retryTransaction(ctx, db, nil, func() bool { cancel(); return true }, func(context.Context, database.Tx) error { attempts++; return conflict })
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, attempts)
	})
}

func TestPrimaryTransactionOptions(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, name := range []string{"PutObject", "AppendObject", "CreateBucket", "PutObjectRetention", "DeleteObject", "UploadPart", "TransitionObjectStorageClass"} {
		require.Nil(t, primaryTransactionOptions(name), name)
	}
	for _, name := range []string{"CopyObject", "CompleteMultipartUpload", "UploadPartCopy"} {
		require.Equal(t, sql.LevelRepeatableRead, primaryTransactionOptions(name).Isolation)
	}
}
