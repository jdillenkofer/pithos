package database

import (
	"context"
	"database/sql"
)

type TxContext struct {
	tx         *sql.Tx
	onCommit   []func(context.Context) error
	onRollback []func(context.Context) error
	finalized  bool
}

func NewTxContext(tx *sql.Tx) *TxContext {
	return &TxContext{tx: tx}
}

func (t *TxContext) SqlTx() *sql.Tx {
	return t.tx
}

func (t *TxContext) RegisterOnCommit(fn func(context.Context) error) {
	t.onCommit = append(t.onCommit, fn)
}

func (t *TxContext) RegisterOnRollback(fn func(context.Context) error) {
	t.onRollback = append(t.onRollback, fn)
}

func (t *TxContext) Commit(ctx context.Context) error {
	err := t.tx.Commit()
	if err != nil {
		_ = t.Rollback(ctx)
		return err
	}
	if t.finalized {
		return nil
	}
	t.finalized = true
	for _, fn := range t.onCommit {
		if hookErr := fn(ctx); hookErr != nil {
			return hookErr
		}
	}
	return nil
}

func (t *TxContext) Rollback(ctx context.Context) error {
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
