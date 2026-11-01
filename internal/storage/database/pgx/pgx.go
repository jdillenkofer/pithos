package pgx

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jdillenkofer/pithos/internal/storage/database"

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

// applyDatabaseMigrations migrates through a pool of its own. The migration
// driver pins one connection until it is closed, and closing it closes its
// pool too, so migrating through the storage's pool would take one of its
// connections for good.
func applyDatabaseMigrations(dbUrl string) error {
	db, err := database.OpenSQL("pgx", dbUrl, semconv.DBSystemPostgreSQL)
	if err != nil {
		return err
	}
	m, err := createMigrateInstance(db)
	if err != nil {
		db.Close()
		return err
	}
	err = m.Up()
	sourceErr, dbErr := m.Close()
	if err != nil && err != migrate.ErrNoChange {
		return err
	}
	return errors.Join(sourceErr, dbErr)
}

type pgxDatabase struct {
	*sql.DB
	// defaultIsolation is the server's default isolation level, which
	// transactions that ask for no isolation level get.
	defaultIsolation sql.IsolationLevel
}

func (d *pgxDatabase) SQLDBs() []*sql.DB { return []*sql.DB{d.DB} }

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
		return tx.Nested(opts)
	}
	tx, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	isolation := d.defaultIsolation
	if opts != nil && opts.Isolation != sql.LevelDefault {
		isolation = opts.Isolation
	}
	// READ UNCOMMITTED behaves like READ COMMITTED on PostgreSQL.
	statementSnapshots := isolation == sql.LevelReadCommitted || isolation == sql.LevelReadUncommitted
	return database.NewTxController(tx, d, readOnly).WithStatementSnapshots(statementSnapshots).WithIsolation(isolation), nil
}

const oldestOpenTransactionStmt = "SELECT COUNT(*) FILTER (WHERE state IS NULL), EXTRACT(EPOCH FROM clock_timestamp() - MIN(xact_start))::FLOAT8 FROM pg_stat_activity WHERE datname = current_database() AND backend_type = 'client backend' AND pid <> pg_backend_pid()"

// OldestOpenTransactionAge reads pg_stat_activity, so it sees the
// transactions of every instance sharing the database. Sessions of other
// roles hide their state unless the role has pg_read_all_stats; then the
// answer is unknown. The age is measured on the database clock.
func (d *pgxDatabase) OldestOpenTransactionAge(ctx context.Context) (time.Duration, bool, error) {
	var hidden int64
	var seconds sql.NullFloat64
	if err := d.DB.QueryRowContext(ctx, oldestOpenTransactionStmt).Scan(&hidden, &seconds); err != nil {
		return 0, false, err
	}
	if hidden > 0 {
		return 0, false, fmt.Errorf("%w: %d sessions", database.ErrOpenTransactionsNotVisible, hidden)
	}
	if !seconds.Valid {
		return 0, false, nil
	}
	return time.Duration(seconds.Float64 * float64(time.Second)), true, nil
}

func (d *pgxDatabase) GetDatabaseType() database.DatabaseType {
	return database.DB_TYPE_POSTGRES
}

func OpenDatabase(dbUrl string, poolConfig *ConnectionPoolConfiguration) (*pgxDatabase, error) {
	if err := applyDatabaseMigrations(dbUrl); err != nil {
		return nil, err
	}
	db, err := database.OpenSQL("pgx", dbUrl, semconv.DBSystemPostgreSQL)
	if err != nil {
		return nil, err
	}
	configureConnectionPool(db, poolConfig)
	defaultIsolation, err := readDefaultIsolation(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	pgxDatabase := pgxDatabase{DB: db, defaultIsolation: defaultIsolation}
	return &pgxDatabase, nil
}

const defaultTransactionIsolationStmt = "SHOW default_transaction_isolation"

// readDefaultIsolation reads the default isolation level once. It is set
// per database or role, which every pooled connection shares.
func readDefaultIsolation(db *sql.DB) (sql.IsolationLevel, error) {
	var isolation string
	if err := db.QueryRow(defaultTransactionIsolationStmt).Scan(&isolation); err != nil {
		return sql.LevelDefault, fmt.Errorf("reading default transaction isolation: %w", err)
	}
	switch isolation {
	case "read uncommitted":
		return sql.LevelReadUncommitted, nil
	case "read committed":
		return sql.LevelReadCommitted, nil
	case "repeatable read":
		return sql.LevelRepeatableRead, nil
	case "serializable":
		return sql.LevelSerializable, nil
	default:
		return sql.LevelDefault, fmt.Errorf("unknown default transaction isolation %q", isolation)
	}
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

