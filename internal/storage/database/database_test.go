package database

import (
	"database/sql"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestMigrateUp(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.Nil(t, err)

	sourceDriver, err := iofs.New(migrationsFilesystem, sqlite.SQLITE_MIGRATION_PATH)
	assert.Nil(t, err)

	databaseDriver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	assert.Nil(t, err)

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite3", databaseDriver)
	assert.Nil(t, err)

	err = m.Up()
	if err != nil {
		assert.Fail(t, err.Error())
	}
}

func TestMigrateUpAndDown(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.Nil(t, err)

	sourceDriver, err := iofs.New(migrationsFilesystem, sqlite.SQLITE_MIGRATION_PATH)
	assert.Nil(t, err)

	databaseDriver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	assert.Nil(t, err)

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite3", databaseDriver)
	assert.Nil(t, err)

	err = m.Up()
	if err != nil {
		assert.Fail(t, err.Error())
	}

	err = m.Down()
	if err != nil {
		assert.Fail(t, err.Error())
	}
}

func TestMigrateUpAndDownAndUp(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.Nil(t, err)

	sourceDriver, err := iofs.New(migrationsFilesystem, sqlite.SQLITE_MIGRATION_PATH)
	assert.Nil(t, err)

	databaseDriver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	assert.Nil(t, err)

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite3", databaseDriver)
	assert.Nil(t, err)

	err = m.Up()
	if err != nil {
		assert.Fail(t, err.Error())
	}

	err = m.Down()
	if err != nil {
		assert.Fail(t, err.Error())
	}

	err = m.Up()
	if err != nil {
		assert.Fail(t, err.Error())
	}
}
