package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
)

// ErrWriteInReadOnlyTransaction is returned by BeginTx when a writable
// transaction is requested while a read-only transaction is already active on
// the same database in the context. Reusing the read-only transaction for
// writes would fail with an opaque driver error (and on SQLite would target the
// read-only connection pool), so the mismatch is rejected up front.
var ErrWriteInReadOnlyTransaction = errors.New("cannot begin writable transaction inside read-only transaction")

// ErrIsolationTooWeak is returned by BeginTx when the requested isolation
// level is stricter than that of the transaction already active on the same
// database in the context, which BeginTx would otherwise reuse silently.
var ErrIsolationTooWeak = errors.New("cannot begin transaction with stricter isolation inside a weaker transaction")

type Tx interface {
	SqlTx() *sql.Tx
	DBHandle() any
	OnAfterCommit(fn func(context.Context) error)
}

type TxController struct {
	tx *sql.Tx
	// hooksMu guards after-commit hooks on the root controller. Fan-out writes
	// (for example, erasure-coding shards) may register hooks concurrently.
	hooksMu          sync.Mutex
	onAfterCommit    []func(context.Context) error
	finalized        bool
	ownsFinalization bool
	readOnly         bool
	dbHandle         any
	root             *TxController
	// startedAt is taken before the transaction's first statement, so it
	// precedes every snapshot the transaction reads from.
	startedAt time.Time
	// statementSnapshots is set when each statement reads the latest
	// committed state (PostgreSQL READ COMMITTED, SQLite writers) instead of
	// one snapshot taken at the first statement.
	statementSnapshots bool
	// isolation is the effective isolation level of the transaction; the
	// database resolves sql.LevelDefault to the level it applies.
	isolation sql.IsolationLevel
}

func NewTx(tx *sql.Tx) *TxController {
	return NewTxController(tx, nil, false)
}

func NewTxController(tx *sql.Tx, dbHandle any, readOnly bool) *TxController {
	t := &TxController{tx: tx, ownsFinalization: true, readOnly: readOnly, dbHandle: dbHandle, startedAt: time.Now()}
	t.root = t
	return t
}

// WithStatementSnapshots records whether statements of the transaction read
// the latest committed state. See SnapshotTime.
func (t *TxController) WithStatementSnapshots(statementSnapshots bool) *TxController {
	t.rootTx().statementSnapshots = statementSnapshots
	return t
}

// StatementSnapshots reports whether each statement of the (root)
// transaction reads the latest committed state.
func (t *TxController) StatementSnapshots() bool {
	return t.rootTx().statementSnapshots
}

// WithStartedAt records when the transaction began, for a controller that
// wraps one begun earlier. See SnapshotTime.
func (t *TxController) WithStartedAt(startedAt time.Time) *TxController {
	t.rootTx().startedAt = startedAt
	return t
}

// WithIsolation records the effective isolation level of the transaction.
func (t *TxController) WithIsolation(isolation sql.IsolationLevel) *TxController {
	t.rootTx().isolation = isolation
	return t
}

// Isolation returns the effective isolation level of the (root) transaction.
func (t *TxController) Isolation() sql.IsolationLevel {
	return t.rootTx().isolation
}

// isolationRank orders isolation levels by strength.
func isolationRank(level sql.IsolationLevel) int {
	switch level {
	case sql.LevelReadUncommitted:
		return 1
	case sql.LevelReadCommitted, sql.LevelWriteCommitted:
		return 2
	case sql.LevelRepeatableRead, sql.LevelSnapshot:
		return 3
	case sql.LevelSerializable:
		return 4
	case sql.LevelLinearizable:
		return 5
	default:
		return 0
	}
}

// Nested returns a child of the active transaction t for a BeginTx with
// opts. It rejects a writable transaction inside a read-only one and an
// isolation level stricter than t's; sql.LevelDefault accepts t's level.
// Nil opts ask for a writable transaction.
func (t *TxController) Nested(opts *sql.TxOptions) (*TxController, error) {
	if (opts == nil || !opts.ReadOnly) && t.ReadOnly() {
		return nil, ErrWriteInReadOnlyTransaction
	}
	if opts != nil && opts.Isolation != sql.LevelDefault && isolationRank(opts.Isolation) > isolationRank(t.Isolation()) {
		return nil, fmt.Errorf("%w: requested %v, active %v", ErrIsolationTooWeak, opts.Isolation, t.Isolation())
	}
	return t.Child(), nil
}

// StartedAt returns when the (root) transaction began.
func (t *TxController) StartedAt() time.Time {
	return t.rootTx().startedAt
}

func (t *TxController) Child() *TxController {
	root := t.rootTx()
	return &TxController{tx: t.tx, ownsFinalization: false, readOnly: root.readOnly, dbHandle: root.dbHandle, root: root}
}

// ReadOnly reports whether the active (root) transaction was begun read-only.
func (t *TxController) ReadOnly() bool {
	return t.rootTx().readOnly
}

func (t *TxController) rootTx() *TxController {
	if t.root == nil {
		return t
	}
	return t.root
}

func (t *TxController) SqlTx() *sql.Tx {
	return t.tx
}

func (t *TxController) DBHandle() any {
	return t.rootTx().dbHandle
}

func (t *TxController) OnAfterCommit(fn func(context.Context) error) {
	root := t.rootTx()
	root.hooksMu.Lock()
	defer root.hooksMu.Unlock()
	root.onAfterCommit = append(root.onAfterCommit, fn)
}

func (t *TxController) Commit(ctx context.Context) error {
	if !t.ownsFinalization {
		return nil
	}
	if t.finalized {
		return nil
	}
	err := t.tx.Commit()
	if err != nil {
		_ = t.Rollback(ctx)
		return err
	}
	t.finalized = true
	for _, fn := range t.onAfterCommit {
		if hookErr := fn(ctx); hookErr != nil {
			return hookErr
		}
	}
	return nil
}

func (t *TxController) Rollback(ctx context.Context) error {
	if !t.ownsFinalization {
		return nil
	}
	t.finalized = true
	return t.tx.Rollback()
}

type txContextKey struct{}

func ContextWithTx(ctx context.Context, tx *TxController) context.Context {
	return context.WithValue(ctx, txContextKey{}, tx.rootTx())
}

func TxControllerFromContext(ctx context.Context) (*TxController, bool) {
	tx, ok := ctx.Value(txContextKey{}).(*TxController)
	if !ok || tx == nil {
		return nil, false
	}
	return tx, true
}

// ContextWithoutTx hides any transaction in ctx, so BeginTx starts a new one.
func ContextWithoutTx(ctx context.Context) context.Context {
	return context.WithValue(ctx, txContextKey{}, (*TxController)(nil))
}

// AmbientTx returns the transaction in ctx that BeginTx on db reuses, if
// any. A transaction of db or of a database db wraps counts, because
// wrappers reuse it in BeginTx; one of another database does not.
func AmbientTx(ctx context.Context, db Database) (*TxController, bool) {
	tx, ok := TxControllerFromContext(ctx)
	if !ok {
		return nil, false
	}
	for db != nil {
		if tx.DBHandle() == any(db) {
			return tx, true
		}
		unwrapper, ok := db.(interface{ UnwrapDatabase() Database })
		if !ok {
			break
		}
		db = unwrapper.UnwrapDatabase()
	}
	return nil, false
}

// SnapshotTime returns a time no later than the state of db that reads in
// ctx observe. Without an ambient transaction of db, or when its statements
// read the latest committed state, that is now: callers take it before they
// begin their own transaction. A transaction that reads one snapshot may
// have taken it as early as its start.
func SnapshotTime(ctx context.Context, db Database) time.Time {
	if tx, ok := AmbientTx(ctx, db); ok && !tx.StatementSnapshots() {
		return tx.StartedAt()
	}
	return time.Now()
}

func WithTx(ctx context.Context, db Database, opts *sql.TxOptions, fn func(ctx context.Context, tx Tx) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	ctx = ContextWithTx(ctx, tx)
	if err = fn(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	return tx.Commit(ctx)
}

// WithTxReadClosers begins a transaction and invokes fn within it, exposing the
// transaction through the context like WithTx does. Unlike WithTx, the
// transaction is not finalized when fn returns: its lifetime is instead bound to
// the io.ReadClosers fn produces. The transaction is rolled back once every
// returned reader has been closed. If fn returns an error, or returns no
// readers, the transaction is rolled back before WithTxReadClosers returns.
//
// This suits read-only streaming reads whose data is fetched lazily from the
// transaction as the returned readers are consumed. fn is responsible for
// closing any readers it already created when it returns an error.
func WithTxReadClosers(ctx context.Context, db Database, opts *sql.TxOptions, fn func(ctx context.Context, tx Tx) ([]io.ReadCloser, error)) ([]io.ReadCloser, error) {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	ctx = ContextWithTx(ctx, tx)
	readers, err := fn(ctx, tx)
	if err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return nil, rollbackErr
		}
		return nil, err
	}

	remaining := int64(len(readers))
	if remaining == 0 {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return nil, rollbackErr
		}
		return readers, nil
	}

	for i := range readers {
		readers[i] = ioutils.NewReadCloserWithCloseHook(readers[i], func() error {
			if atomic.AddInt64(&remaining, -1) == 0 {
				return tx.Rollback(ctx)
			}
			return nil
		})
	}
	return readers, nil
}
