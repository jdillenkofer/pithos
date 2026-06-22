package database

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/jdillenkofer/pithos/internal/ioutils"
)

// ErrWriteInReadOnlyTransaction is returned by BeginTx when a writable
// transaction is requested while a read-only transaction is already active on
// the same database in the context. Reusing the read-only transaction for
// writes would fail with an opaque driver error (and on SQLite would target the
// read-only connection pool), so the mismatch is rejected up front.
var ErrWriteInReadOnlyTransaction = errors.New("cannot begin writable transaction inside read-only transaction")

type Tx interface {
	SqlTx() *sql.Tx
	DBHandle() any
	OnPreCommit(fn func(context.Context) error)
	OnAfterCommit(fn func(context.Context) error)
	OnRollback(fn func(context.Context) error)
}

type TxController struct {
	tx               *sql.Tx
	// hooksMu guards the hook slices on the root controller; a single tx can be
	// shared across fan-out goroutines (e.g. erasure-coding PutPart writes each
	// shard concurrently), so OnPreCommit/OnAfterCommit/OnRollback may register
	// hooks in parallel.
	hooksMu          sync.Mutex
	onPreCommit      []func(context.Context) error
	onAfterCommit    []func(context.Context) error
	onRollback       []func(context.Context) error
	finalized        bool
	ownsFinalization bool
	readOnly         bool
	dbHandle         any
	root             *TxController
}

func NewTx(tx *sql.Tx) *TxController {
	return NewTxController(tx, nil, false)
}

func NewTxController(tx *sql.Tx, dbHandle any, readOnly bool) *TxController {
	t := &TxController{tx: tx, ownsFinalization: true, readOnly: readOnly, dbHandle: dbHandle}
	t.root = t
	return t
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

func (t *TxController) OnPreCommit(fn func(context.Context) error) {
	root := t.rootTx()
	root.hooksMu.Lock()
	defer root.hooksMu.Unlock()
	root.onPreCommit = append(root.onPreCommit, fn)
}

func (t *TxController) OnAfterCommit(fn func(context.Context) error) {
	root := t.rootTx()
	root.hooksMu.Lock()
	defer root.hooksMu.Unlock()
	root.onAfterCommit = append(root.onAfterCommit, fn)
}

func (t *TxController) OnRollback(fn func(context.Context) error) {
	root := t.rootTx()
	root.hooksMu.Lock()
	defer root.hooksMu.Unlock()
	root.onRollback = append(root.onRollback, fn)
}

func (t *TxController) Commit(ctx context.Context) error {
	if !t.ownsFinalization {
		return nil
	}
	if t.finalized {
		return nil
	}
	for _, fn := range t.onPreCommit {
		if hookErr := fn(ctx); hookErr != nil {
			_ = t.Rollback(ctx)
			return hookErr
		}
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
	err := t.tx.Rollback()
	if t.finalized {
		return err
	}
	t.finalized = true
	for _, fn := range t.onRollback {
		if hookErr := fn(ctx); hookErr != nil && err == nil {
			err = hookErr
		}
	}
	return err
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
