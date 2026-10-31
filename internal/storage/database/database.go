package database

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type DatabaseType uint

const (
	DB_TYPE_SQLITE DatabaseType = iota
	DB_TYPE_POSTGRES
)

type Database interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*TxController, error)
	PingContext(ctx context.Context) error
	Close() error
	GetDatabaseType() DatabaseType
}

// OpenTransactionReporter reports the oldest transaction open on a database,
// across every process that uses it.
type OpenTransactionReporter interface {
	// OldestOpenTransactionAge returns how long the oldest transaction that
	// may still write has been open; ok is false when there is none. It
	// returns ErrOpenTransactionsNotVisible when it cannot see them all.
	OldestOpenTransactionAge(ctx context.Context) (age time.Duration, ok bool, err error)
}

var ErrOpenTransactionsNotVisible = errors.New("transactions of other database sessions are not visible")
