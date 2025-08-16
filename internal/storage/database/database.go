package database

import (
	"context"
	"database/sql"
	"errors"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jdillenkofer/pithos/internal/storage/database/pgx"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
)

type DatabaseType uint

const (
	DB_TYPE_SQLITE DatabaseType = iota
	DB_TYPE_POSTGRES
)

var errUnknownDatabaseType = errors.New("unknown database type")

type Database interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	PingContext(ctx context.Context) error
	Close() error
}

func OpenDatabase(dbType DatabaseType, dbUrl string) (Database, error) {
	switch dbType {
	case DB_TYPE_SQLITE:
		return sqlite.OpenDatabase(dbUrl)
	case DB_TYPE_POSTGRES:
		return pgx.OpenDatabase(dbUrl)
	default:
		return nil, errUnknownDatabaseType
	}
}
