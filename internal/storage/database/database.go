package database

import (
	"context"
	"database/sql"
	"embed"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/**/*.sql
var migrationsFilesystem embed.FS

// In auto-vacuum full mode freelist pages are moved to the end of the file
// end the file is truncated
// See https://www.sqlite.org/pragma.html#pragma_auto_vacuum
func enableAutoVacuumFullMode(db *sql.DB) error {
	enableAutoVacuumFullStmt := "PRAGMA auto_vacuum = FULL;"

	_, err := db.Exec(enableAutoVacuumFullStmt)
	if err != nil {
		return err
	}
	return nil
}

func enableWALJournalMode(db *sql.DB) error {
	enableWalJournalModeStmt := "PRAGMA journal_mode = WAL;"

	_, err := db.Exec(enableWalJournalModeStmt)
	if err != nil {
		return err
	}
	return nil
}

func enableNormalSynchronous(db *sql.DB) error {
	enableNormalSynchronousStmt := "PRAGMA synchronous = NORMAL;"

	_, err := db.Exec(enableNormalSynchronousStmt)
	if err != nil {
		return err
	}
	return nil
}

func enableForeignKeyConstraints(db *sql.DB) error {
	enableForeignKeysStmt := "PRAGMA foreign_keys = ON;"

	_, err := db.Exec(enableForeignKeysStmt)
	if err != nil {
		return err
	}
	return nil
}

func applyDatabaseMigrations(db *sql.DB) error {
	sourceDriver, err := iofs.New(migrationsFilesystem, "migrations/sqlite")
	if err != nil {
		return err
	}

	databaseDriver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return err
	}
	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite3", databaseDriver)
	if err != nil {
		return err
	}
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

type Database interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	PingContext(ctx context.Context) error
	Close() error
}

type sqliteDatabase struct {
	readOnlyDb  *sql.DB
	writeableDb *sql.DB
}

func (sdb *sqliteDatabase) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if opts.ReadOnly {
		return sdb.readOnlyDb.BeginTx(ctx, opts)
	}
	return sdb.writeableDb.BeginTx(ctx, opts)
}

func (sdb *sqliteDatabase) PingContext(ctx context.Context) error {
	return sdb.readOnlyDb.PingContext(ctx)
}

func (sdb *sqliteDatabase) Close() error {
	err := sdb.readOnlyDb.Close()
	if err != nil {
		return err
	}
	err = sdb.writeableDb.Close()
	if err != nil {
		return err
	}
	return nil
}

func OpenDatabase(dbPath string) (Database, error) {
	storagePath := filepath.Dir(dbPath)
	err := os.MkdirAll(storagePath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	writeableDb, err := sql.Open("sqlite3", dbPath+"?mode=rwc&_busy_timeout=5000&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	err = setupWriteableDatabase(writeableDb)
	if err != nil {
		writeableDb.Close()
		return nil, err
	}

	readOnlyDb, err := sql.Open("sqlite3", dbPath+"?mode=ro&_busy_timeout=5000&_txlock=deferred")
	if err != nil {
		writeableDb.Close()
		return nil, err
	}
	sqliteDatabase := sqliteDatabase{readOnlyDb, writeableDb}
	return &sqliteDatabase, nil
}

func setupWriteableDatabase(db *sql.DB) error {
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
	err := enableAutoVacuumFullMode(db)
	if err != nil {
		return err
	}
	err = enableWALJournalMode(db)
	if err != nil {
		return err
	}
	err = enableNormalSynchronous(db)
	if err != nil {
		return err
	}
	err = enableForeignKeyConstraints(db)
	if err != nil {
		return err
	}
	err = applyDatabaseMigrations(db)
	if err != nil {
		return err
	}
	return nil
}
