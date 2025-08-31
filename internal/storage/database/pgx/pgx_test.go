package pgx

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func setupPostgresContainer(ctx context.Context) (*postgres.PostgresContainer, error) {
	username := "postgres"
	password := "postgres"
	dbname := "postgres"
	postgresContainer, err := postgres.Run(ctx, "postgres:17.5-alpine3.22",
		postgres.WithUsername(username),
		postgres.WithPassword(password),
		postgres.WithDatabase(dbname),
		postgres.BasicWaitStrategies())
	if err != nil {
		return nil, err
	}
	return postgresContainer, nil
}

func TestMigrateUp(t *testing.T) {
	testutils.SkipIfIntegration(t)

	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := t.Context()
	pgContainer, err := setupPostgresContainer(ctx)
	assert.Nil(t, err)
	dbUrl, err := pgContainer.ConnectionString(ctx)
	assert.Nil(t, err)
	defer pgContainer.Terminate(ctx)

	db, err := sql.Open("pgx", dbUrl)
	assert.Nil(t, err)
	defer db.Close()

	m, err := createMigrateInstance(db)
	assert.Nil(t, err)

	err = m.Up()
	if err != nil {
		assert.Fail(t, err.Error())
	}
}

func TestMigrateUpAndDown(t *testing.T) {
	testutils.SkipIfIntegration(t)

	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := t.Context()
	pgContainer, err := setupPostgresContainer(ctx)
	assert.Nil(t, err)
	dbUrl, err := pgContainer.ConnectionString(ctx)
	assert.Nil(t, err)
	defer pgContainer.Terminate(ctx)

	db, err := sql.Open("pgx", dbUrl)
	assert.Nil(t, err)
	defer db.Close()

	m, err := createMigrateInstance(db)
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
	testutils.SkipIfIntegration(t)

	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := t.Context()
	pgContainer, err := setupPostgresContainer(ctx)
	assert.Nil(t, err)
	dbUrl, err := pgContainer.ConnectionString(ctx)
	assert.Nil(t, err)
	defer pgContainer.Terminate(ctx)

	db, err := sql.Open("pgx", dbUrl)
	assert.Nil(t, err)
	defer db.Close()

	m, err := createMigrateInstance(db)
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
