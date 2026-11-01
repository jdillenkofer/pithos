package gc

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type recordingDatabase struct {
	database.Database
	begun *database.TxController
}

func (db *recordingDatabase) BeginTx(ctx context.Context, opts *sql.TxOptions) (*database.TxController, error) {
	tx, err := db.Database.BeginTx(ctx, opts)
	db.begun = tx
	return tx, err
}

// SnapshotTime relies on StartedAt preceding every snapshot, so the wrapper
// must report when the inner transaction began, not when it wrapped it.
func TestProtectedDatabaseKeepsStartTimeOfInnerTransaction(t *testing.T) {
	testutils.SkipIfIntegration(t)
	env := newSqliteGCTestEnv(t)
	inner := &recordingDatabase{Database: env.db}
	db := NewProtectedDatabase(inner, env.collector)
	for _, readOnly := range []bool{true, false} {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: readOnly})
		require.NoError(t, err)
		require.Equal(t, inner.begun.StartedAt(), tx.StartedAt())
		require.Equal(t, inner.begun.StatementSnapshots(), tx.StatementSnapshots())
		require.NoError(t, tx.Rollback(context.Background()))
	}
}
