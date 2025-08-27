package database

import (
	"context"
	"database/sql"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type DatabaseType uint

const (
	DB_TYPE_SQLITE DatabaseType = iota
	DB_TYPE_POSTGRES
)

type Database interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	PingContext(ctx context.Context) error
	Close() error
	GetDatabaseType() DatabaseType
}
