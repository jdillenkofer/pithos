package database_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

func openTestDB(t *testing.T) database.Database {
	t.Helper()
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func execTx(t *testing.T, ctx context.Context, db database.Database, stmt string, args ...any) {
	t.Helper()
	if err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		_, err := tx.SqlTx().ExecContext(ctx, stmt, args...)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func countRows(t *testing.T, ctx context.Context, db database.Database, table string) int {
	t.Helper()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	var count int
	if err := tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestWithTxRunsPreCommitBeforeCommitAndAfterCommitAfterCommit(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	db := openTestDB(t)
	execTx(t, ctx, db, "CREATE TABLE tx_lifecycle (id INTEGER PRIMARY KEY)")

	var order []string
	if err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		tx.OnPreCommit(func(context.Context) error {
			order = append(order, "pre")
			return nil
		})
		tx.OnAfterCommit(func(context.Context) error {
			order = append(order, "after")
			return nil
		})
		order = append(order, "body")
		_, err := tx.SqlTx().ExecContext(ctx, "INSERT INTO tx_lifecycle (id) VALUES (1)")
		return err
	}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(order, []string{"body", "pre", "after"}) {
		t.Fatalf("unexpected hook order: %v", order)
	}
	if got := countRows(t, ctx, db, "tx_lifecycle"); got != 1 {
		t.Fatalf("expected committed row, got %d", got)
	}
}

func TestWithTxPreCommitErrorRollsBackAndRunsRollbackHooks(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	db := openTestDB(t)
	execTx(t, ctx, db, "CREATE TABLE tx_precommit_rollback (id INTEGER PRIMARY KEY)")

	preCommitErr := errors.New("precommit failed")
	rollbackRan := false
	err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		tx.OnPreCommit(func(context.Context) error {
			return preCommitErr
		})
		tx.OnAfterCommit(func(context.Context) error {
			t.Fatal("after-commit hook ran after pre-commit failure")
			return nil
		})
		tx.OnRollback(func(context.Context) error {
			rollbackRan = true
			return nil
		})
		_, err := tx.SqlTx().ExecContext(ctx, "INSERT INTO tx_precommit_rollback (id) VALUES (1)")
		return err
	})
	if !errors.Is(err, preCommitErr) {
		t.Fatalf("expected precommit error, got %v", err)
	}
	if !rollbackRan {
		t.Fatal("expected rollback hook to run")
	}
	if got := countRows(t, ctx, db, "tx_precommit_rollback"); got != 0 {
		t.Fatalf("expected rollback to remove row, got %d rows", got)
	}
}

func TestNestedWritableTxInsideReadOnlyTxIsRejected(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	db := openTestDB(t)
	execTx(t, ctx, db, "CREATE TABLE tx_ro_guard (id INTEGER PRIMARY KEY)")

	err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, _ database.Tx) error {
		_, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
		return err
	})
	if !errors.Is(err, database.ErrWriteInReadOnlyTransaction) {
		t.Fatalf("expected ErrWriteInReadOnlyTransaction, got %v", err)
	}
}

func TestNestedReadOnlyTxInsideReadOnlyTxIsAllowed(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	db := openTestDB(t)
	execTx(t, ctx, db, "CREATE TABLE tx_ro_nested (id INTEGER PRIMARY KEY)")

	if err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, outer database.Tx) error {
		inner, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return err
		}
		if inner.SqlTx() != outer.SqlTx() {
			t.Fatal("expected nested read-only transaction to reuse active SQL transaction")
		}
		return inner.Commit(ctx)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNestedBeginTxReusesActiveTransactionWithoutOwningCommit(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	db := openTestDB(t)
	execTx(t, ctx, db, "CREATE TABLE tx_nested (id INTEGER PRIMARY KEY)")

	var order []string
	if err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, outer database.Tx) error {
		inner, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
		if err != nil {
			return err
		}
		if inner.SqlTx() != outer.SqlTx() {
			t.Fatal("expected nested transaction to reuse active SQL transaction")
		}
		inner.OnAfterCommit(func(context.Context) error {
			order = append(order, "inner-after")
			return nil
		})
		if _, err := inner.SqlTx().ExecContext(ctx, "INSERT INTO tx_nested (id) VALUES (1)"); err != nil {
			return err
		}
		if err := inner.Commit(ctx); err != nil {
			return err
		}
		if got := countRows(t, context.Background(), db, "tx_nested"); got != 0 {
			t.Fatalf("inner commit should not commit outer transaction, got %d rows", got)
		}
		order = append(order, "body")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(order, []string{"body", "inner-after"}) {
		t.Fatalf("unexpected nested hook order: %v", order)
	}
	if got := countRows(t, ctx, db, "tx_nested"); got != 1 {
		t.Fatalf("expected outer commit to persist row, got %d", got)
	}
}
