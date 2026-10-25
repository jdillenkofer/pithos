package replication

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/stretchr/testify/require"
)

type commitFaultDriver struct{}
type commitFaultConn struct {
	failure         error
	rollbackFailure error
}
type commitFaultTx struct{ conn *commitFaultConn }

func (commitFaultDriver) Open(string) (driver.Conn, error) { return &commitFaultConn{}, nil }
func (*commitFaultConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}
func (*commitFaultConn) Close() error                { return nil }
func (c *commitFaultConn) Begin() (driver.Tx, error) { return &commitFaultTx{c}, nil }
func (t *commitFaultTx) Commit() error               { return t.conn.failure }
func (t *commitFaultTx) Rollback() error             { return t.conn.rollbackFailure }
func init()                                          { sql.Register("replication-commit-fault", commitFaultDriver{}) }

type commitFaultDB struct {
	database.Database
	db *sql.DB
}

func (d *commitFaultDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*database.TxController, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return database.NewTxController(tx, d, false), nil
}

func TestRetryTransactionDoesNotReplayFailedCommit(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, failure := range []error{&pgconn.PgError{Code: "40001"}, errors.New("connection lost after COMMIT")} {
		t.Run(failure.Error(), func(t *testing.T) {
			db, err := sql.Open("replication-commit-fault", "")
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)
			conn, err := db.Conn(t.Context())
			require.NoError(t, err)
			require.NoError(t, conn.Raw(func(raw any) error { raw.(*commitFaultConn).failure = failure; return nil }))
			require.NoError(t, conn.Close())
			attempts, hooks := 0, 0
			err = retryTransaction(t.Context(), &commitFaultDB{db: db}, nil, func() bool { return true }, func(ctx context.Context, tx database.Tx) error {
				attempts++
				tx.OnAfterCommit(func(context.Context) error { hooks++; return nil })
				return nil
			})
			require.ErrorIs(t, err, failure)
			require.Equal(t, 1, attempts)
			require.Zero(t, hooks)
		})
	}
}

func TestRetryTransactionRequiresConfirmedRollback(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, err := sql.Open("replication-commit-fault", "")
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	rollbackFailure := errors.New("rollback connection failure")
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	require.NoError(t, conn.Raw(func(raw any) error { raw.(*commitFaultConn).rollbackFailure = rollbackFailure; return nil }))
	require.NoError(t, conn.Close())
	attempts := 0
	err = retryTransaction(t.Context(), &commitFaultDB{db: db}, nil, func() bool { return true }, func(context.Context, database.Tx) error { attempts++; return &pgconn.PgError{Code: "40001"} })
	require.ErrorIs(t, err, rollbackFailure)
	require.Equal(t, 1, attempts)
}
