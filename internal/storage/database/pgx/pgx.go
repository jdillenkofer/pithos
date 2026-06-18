package pgx

import (
	"context"
	"database/sql"
	"embed"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jdillenkofer/pithos/internal/storage/database"

	"github.com/XSAM/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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

type pgxDatabase struct {
	*sql.DB
}

const (
	defaultMaxOpenConns    = 20
	defaultMaxIdleConns    = 20
	defaultConnMaxLifetime = 30 * time.Minute
	defaultConnMaxIdleTime = 5 * time.Minute
)

type ConnectionPoolConfiguration struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

func (d *pgxDatabase) BeginTx(ctx context.Context, opts *sql.TxOptions) (*database.TxController, error) {
	readOnly := opts != nil && opts.ReadOnly
	if tx, ok := database.TxControllerFromContext(ctx); ok && tx.DBHandle() == d {
		if !readOnly && tx.ReadOnly() {
			return nil, database.ErrWriteInReadOnlyTransaction
		}
		return tx.Child(), nil
	}
	tx, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return database.NewTxController(tx, d, readOnly), nil
}

func (d *pgxDatabase) GetDatabaseType() database.DatabaseType {
	return database.DB_TYPE_POSTGRES
}

func OpenDatabase(dbUrl string, poolConfig *ConnectionPoolConfiguration) (*pgxDatabase, error) {
	db, err := otelsql.Open("pgx", dbUrl,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
	)
	if err != nil {
		return nil, err
	}
	configureConnectionPool(db, poolConfig)
	err = setupDatabase(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	pgxDatabase := pgxDatabase{db}
	return &pgxDatabase, nil
}

func configureConnectionPool(db *sql.DB, poolConfigOverride *ConnectionPoolConfiguration) {
	cfg := defaultPoolConfiguration()
	if poolConfigOverride != nil {
		cfg = mergePoolConfiguration(cfg, *poolConfigOverride)
	}
	cfg = normalizePoolConfiguration(cfg)

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
}

func mergePoolConfiguration(base ConnectionPoolConfiguration, override ConnectionPoolConfiguration) ConnectionPoolConfiguration {
	if override.MaxOpenConns > 0 {
		base.MaxOpenConns = override.MaxOpenConns
	}
	if override.MaxIdleConns > 0 {
		base.MaxIdleConns = override.MaxIdleConns
	}
	if override.ConnMaxLifetime > 0 {
		base.ConnMaxLifetime = override.ConnMaxLifetime
	}
	if override.ConnMaxIdleTime > 0 {
		base.ConnMaxIdleTime = override.ConnMaxIdleTime
	}
	return base
}

func normalizePoolConfiguration(cfg ConnectionPoolConfiguration) ConnectionPoolConfiguration {
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = defaultMaxOpenConns
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = defaultMaxIdleConns
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		cfg.MaxIdleConns = cfg.MaxOpenConns
	}
	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = defaultConnMaxLifetime
	}
	if cfg.ConnMaxIdleTime <= 0 {
		cfg.ConnMaxIdleTime = defaultConnMaxIdleTime
	}
	return cfg
}

func defaultPoolConfiguration() ConnectionPoolConfiguration {
	return ConnectionPoolConfiguration{
		MaxOpenConns:    defaultMaxOpenConns,
		MaxIdleConns:    defaultMaxIdleConns,
		ConnMaxLifetime: defaultConnMaxLifetime,
		ConnMaxIdleTime: defaultConnMaxIdleTime,
	}
}

func setupDatabase(db *sql.DB) error {
	err := applyDatabaseMigrations(db)
	if err != nil {
		return err
	}
	return nil
}
