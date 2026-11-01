package database

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"
)

// RetryableTxErrorClassifier is implemented by databases whose transactions
// may fail only because they conflicted with concurrent transactions.
type RetryableTxErrorClassifier interface {
	// IsRetryableTxError reports whether err aborted a transaction that may
	// succeed when run again, such as a serialization failure or deadlock.
	IsRetryableTxError(err error) bool
}

const (
	maxTxAttempts    = 5
	txRetryBaseDelay = 10 * time.Millisecond
	txRetryMaxDelay  = 500 * time.Millisecond
)

// afterCommitError reports that a transaction committed and one of its
// after-commit hooks failed, so running the transaction again would repeat
// committed work.
type afterCommitError struct {
	err error
}

func (e *afterCommitError) Error() string { return e.err.Error() }
func (e *afterCommitError) Unwrap() error { return e.err }

// WithTxRetry is WithTx that runs fn again, in a new transaction, when the
// transaction failed only because it conflicted with concurrent ones. fn
// must be safe to run more than once: it may not consume a stream, and any
// state it leaves outside the transaction must be reset on every run.
//
// Inside an active transaction of db there is nothing to retry on its own:
// fn joins that transaction once, and the failure aborts it for the caller.
func WithTxRetry(ctx context.Context, db Database, opts *sql.TxOptions, fn func(ctx context.Context, tx Tx) error) error {
	if _, ok := AmbientTx(ctx, db); ok {
		return WithTx(ctx, db, opts, fn)
	}
	classifier, ok := retryableTxErrorClassifier(db)
	if !ok {
		return WithTx(ctx, db, opts, fn)
	}
	delay := txRetryBaseDelay
	for attempt := 1; ; attempt++ {
		err := WithTx(ctx, db, opts, fn)
		if err == nil || attempt == maxTxAttempts || !isRetryableTxError(classifier, err) {
			return err
		}
		slog.Debug("Retrying transaction after a conflict", "attempt", attempt, "error", err)
		// Jitter keeps conflicting transactions from retrying in lockstep.
		timer := time.NewTimer(delay/2 + rand.N(delay/2+1))
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		delay = min(2*delay, txRetryMaxDelay)
	}
}

func isRetryableTxError(classifier RetryableTxErrorClassifier, err error) bool {
	var committed *afterCommitError
	if errors.As(err, &committed) {
		return false
	}
	return classifier.IsRetryableTxError(err)
}

func retryableTxErrorClassifier(db Database) (RetryableTxErrorClassifier, bool) {
	for db != nil {
		if classifier, ok := db.(RetryableTxErrorClassifier); ok {
			return classifier, true
		}
		unwrapper, ok := db.(interface{ UnwrapDatabase() Database })
		if !ok {
			break
		}
		db = unwrapper.UnwrapDatabase()
	}
	return nil, false
}
