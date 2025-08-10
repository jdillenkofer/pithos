package pgx

import (
	"database/sql"
	"embed"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFilesystem embed.FS

func createMigrateInstance(db *sql.DB) (*migrate.Migrate, error) {
	sourceDriver, err := iofs.New(migrationsFilesystem, "migrations")
	if err != nil {
		return nil, err
	}

	databaseDriver, err := pgx.WithInstance(db, &pgx.Config{})
	if err != nil {
		return nil, err
	}
	m, err := migrate.NewWithInstance("iofs", sourceDriver, "pgx", databaseDriver)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func applyDatabaseMigrations(db *sql.DB) error {
	m, err := createMigrateInstance(db)
	if err != nil {
		return err
	}
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

func OpenDatabase(dbUrl string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dbUrl)
	if err != nil {
		return nil, err
	}
	err = setupDatabase(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func setupDatabase(db *sql.DB) error {
	err := applyDatabaseMigrations(db)
	if err != nil {
		return err
	}
	return nil
}
