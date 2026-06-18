package database

import (
	"context"
	"database/sql"
)

type Tx interface {
	SqlTx() *sql.Tx
	DBHandle() any
	OnPreCommit(fn func(context.Context) error)
	OnAfterCommit(fn func(context.Context) error)
	OnRollback(fn func(context.Context) error)
}

type TxController struct {
	tx               *sql.Tx
	onPreCommit      []func(context.Context) error
	onAfterCommit    []func(context.Context) error
	onRollback       []func(context.Context) error
	finalized        bool
	ownsFinalization bool
	dbHandle         any
	root             *TxController
}

func NewTx(tx *sql.Tx) *TxController {
	return NewTxController(tx, nil)
}

func NewTxController(tx *sql.Tx, dbHandle any) *TxController {
	t := &TxController{tx: tx, ownsFinalization: true, dbHandle: dbHandle}
	t.root = t
	return t
}

func (t *TxController) Child() *TxController {
	root := t.rootTx()
	return &TxController{tx: t.tx, ownsFinalization: false, dbHandle: root.dbHandle, root: root}
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
	root.onPreCommit = append(root.onPreCommit, fn)
}

func (t *TxController) OnAfterCommit(fn func(context.Context) error) {
	root := t.rootTx()
	root.onAfterCommit = append(root.onAfterCommit, fn)
}

func (t *TxController) OnRollback(fn func(context.Context) error) {
	root := t.rootTx()
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
