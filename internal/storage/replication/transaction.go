package replication

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
)

// retryTransaction is deliberately private to replication. fn must recreate all
// attempt-local state and readers. canRetry must become false before invoking
// anything whose effects are not entirely rolled back with this transaction.
// Never retry Commit: its outcome (or a failing after-commit hook) may be durable.
func retryTransaction(ctx context.Context, db database.Database, opts *sql.TxOptions, canRetry func() bool, fn func(context.Context, database.Tx) error) error {
	if _, ambient := database.TxControllerFromContext(ctx); ambient {
		return errors.New("replication retry requires its own transaction")
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, opts)
		if err != nil {
			return err
		}
		attemptCtx := database.ContextWithTx(ctx, tx)
		err = fn(attemptCtx, tx)
		if err == nil {
			return tx.Commit(attemptCtx)
		}
		if rollbackErr := tx.Rollback(attemptCtx); rollbackErr != nil {
			return errors.Join(err, rollbackErr, ctx.Err())
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var state interface{ SQLState() string }
		if attempt >= 2 || !canRetry() || !errors.As(err, &state) || (state.SQLState() != "40001" && state.SQLState() != "40P01") {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Only these operations call GetObject while finalizing the primary. Ordinary
// writes retain READ COMMITTED, avoiding snapshot conflicts on bucket locks and
// registry upserts that do not need a stable read snapshot.
func primaryTransactionOptions(name string) *sql.TxOptions {
	switch name {
	case "CopyObject", "CompleteMultipartUpload", "UploadPartCopy":
		return &sql.TxOptions{Isolation: sql.LevelRepeatableRead}
	default:
		return nil
	}
}
