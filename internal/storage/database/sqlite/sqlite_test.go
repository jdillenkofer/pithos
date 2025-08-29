package sqlite

import (
	"database/sql"
	"testing"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestMigrateUp(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, err := sql.Open("sqlite3", ":memory:")
	assert.Nil(t, err)

	m, err := createMigrateInstance(db)
	assert.Nil(t, err)
	defer db.Close()

	err = m.Up()
	if err != nil {
		assert.Fail(t, err.Error())
	}
}

func TestMigrateUpAndDown(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, err := sql.Open("sqlite3", ":memory:")
	assert.Nil(t, err)

	m, err := createMigrateInstance(db)
	assert.Nil(t, err)
	defer db.Close()

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
	testutils.SkipIfIntegration(t)
	db, err := sql.Open("sqlite3", ":memory:")
	assert.Nil(t, err)

	m, err := createMigrateInstance(db)
	assert.Nil(t, err)
	defer db.Close()

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
