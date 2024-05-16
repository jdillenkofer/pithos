package storage

import (
	"database/sql"
	"embed"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFilesystem embed.FS

func enableForeignKeyConstraints(db *sql.DB) error {
	enableForeignKeysStmt := `
	PRAGMA foreign_keys = ON;
	`
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
	if err != nil {
		return err
	}
	return nil
}

func SetupDatabase(db *sql.DB) error {
	err := enableForeignKeyConstraints(db)
	if err != nil {
		return err
	}
	err = applyDatabaseMigrations(db)
	if err != nil {
		return err
	}
	return nil
}
