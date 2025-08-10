package database

import (
	"context"
	"database/sql"
	"errors"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
)

const (
	DB_TYPE_SQLITE   = "sqlite"
	DB_TYPE_POSTGRES = "postgres"
)

var errUnknownDatabaseType = errors.New("unknown database type")

type Database interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	PingContext(ctx context.Context) error
	Close() error
}

func OpenDatabase(dbType string, dbUrl string) (Database, error) {
	switch dbType {
	case DB_TYPE_SQLITE:
		return sqlite.OpenDatabase(dbUrl)
	case DB_TYPE_POSTGRES:
		// Placeholder for future PostgreSQL implementation
		return nil, errors.New("PostgreSQL support is not implemented yet")
	default:
		return nil, errUnknownDatabaseType
	}
}
