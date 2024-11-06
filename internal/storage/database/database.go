package database

import (
	"database/sql"
	"embed"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
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
	sourceDriver, err := iofs.New(migrationsFilesystem, "migrations")
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

func OpenDatabase(storagePath string) (*sql.DB, error) {
	err := os.MkdirAll(storagePath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", filepath.Join(storagePath, "pithos.db?mode=rwc"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	err = SetupDatabase(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func SetupDatabase(db *sql.DB) error {
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
