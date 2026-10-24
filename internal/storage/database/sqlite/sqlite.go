package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	gosqlite3 "github.com/mattn/go-sqlite3"

	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

//go:embed migrations/*.sql
var migrationsFilesystem embed.FS

// SQLite open/setup is serialized to avoid intermittent cgo crashes when
// multiple test goroutines initialize sqlite connections concurrently.
var openDatabaseMu sync.Mutex

const durableDriverName = "pithos-sqlite3"

func init() {
	sql.Register(durableDriverName, &gosqlite3.SQLiteDriver{
		ConnectHook: func(conn *gosqlite3.SQLiteConn) error {
			// On macOS, request device-cache flushes as well as OS-cache
			// flushes. These pragmas are harmless on other platforms.
			_, err := conn.Exec("PRAGMA fullfsync = ON; PRAGMA checkpoint_fullfsync = ON;", nil)
			return err
		},
	})
}

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

func enableForeignKeyConstraints(db *sql.DB) error {
	enableForeignKeysStmt := "PRAGMA foreign_keys = ON;"

	_, err := db.Exec(enableForeignKeysStmt)
	if err != nil {
		return err
	}
	return nil
}

func createMigrateInstance(db *sql.DB) (*migrate.Migrate, error) {
	sourceDriver, err := iofs.New(migrationsFilesystem, "migrations")
	if err != nil {
		return nil, err
	}

	databaseDriver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return nil, err
	}
	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite3", databaseDriver)
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

type sqliteDatabase struct {
	readOnlyDb  *sql.DB
	writeableDb *sql.DB
}

func (sdb *sqliteDatabase) SQLDBs() []*sql.DB { return []*sql.DB{sdb.readOnlyDb, sdb.writeableDb} }

func (sdb *sqliteDatabase) BeginTx(ctx context.Context, opts *sql.TxOptions) (*database.TxController, error) {
	readOnly := opts != nil && opts.ReadOnly
	if tx, ok := database.TxControllerFromContext(ctx); ok && tx.DBHandle() == sdb {
		if !readOnly && tx.ReadOnly() {
			return nil, database.ErrWriteInReadOnlyTransaction
		}
		return tx.Child(), nil
	}
	if readOnly {
		tx, err := sdb.readOnlyDb.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return database.NewTxController(tx, sdb, readOnly), nil
	}
	tx, err := sdb.writeableDb.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return database.NewTxController(tx, sdb, readOnly), nil
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

func (sdb *sqliteDatabase) GetDatabaseType() database.DatabaseType {
	return database.DB_TYPE_SQLITE
}

// stmtCacheSize is the number of prepared statements go-sqlite3 keeps per
// connection. Without it every query is re-parsed and re-planned by sqlite,
// which dominates the cost of the small indexed lookups on the request path.
// Sized to hold every statement the repositories issue.
const stmtCacheSize = "256"

func OpenDatabase(dbPath string) (*sqliteDatabase, error) {
	openDatabaseMu.Lock()
	defer openDatabaseMu.Unlock()

	storagePath := filepath.Dir(dbPath)
	err := ioutils.EnsureDurableDirectory(storagePath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	writeableDb, err := database.OpenSQL(durableDriverName, dbPath+"?mode=rwc&_busy_timeout=5000&_txlock=immediate&_sync=FULL&_foreign_keys=on&_stmt_cache_size="+stmtCacheSize,
		semconv.DBSystemSqlite,
	)
	if err != nil {
		return nil, err
	}
	err = setupWriteableDatabase(writeableDb)
	if err == nil {
		err = ioutils.SyncDirectory(storagePath)
	}
	if err != nil {
		writeableDb.Close()
		return nil, err
	}

	readOnlyDb, err := database.OpenSQL(durableDriverName, dbPath+"?mode=ro&_busy_timeout=5000&_txlock=deferred&_sync=FULL&_foreign_keys=on&_stmt_cache_size="+stmtCacheSize,
		semconv.DBSystemSqlite,
	)
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
	// Migrations need foreign keys disabled on the sole writer connection.
	// The DSN enables them again for any replacement connection.
	if _, err := db.Exec("PRAGMA foreign_keys = OFF;"); err != nil {
		return err
	}
	err = applyDatabaseMigrations(db)
	if err != nil {
		return err
	}
	err = enableForeignKeyConstraints(db)
	if err != nil {
		return err
	}
	return nil
}
