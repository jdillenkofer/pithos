package metadatapart

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/stretchr/testify/require"
)

// Deletion removes metadata immediately, but physical content and zero-reference
// registry rows must survive until GC retention expires.
func assertRetainedUnreferencedParts(t *testing.T, st *metadataPartStorage) {
	t.Helper()
	require.NotEmpty(t, physicalPartIDs(t, st))
	require.NoError(t, database.WithTx(context.Background(), st.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var live, invalidState int
		require.NoError(t, tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM parts").Scan(&live))
		require.Zero(t, live)
		require.NoError(t, tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM part_registry WHERE ref_count != 0 OR unreferenced_at IS NOT NULL").Scan(&invalidState))
		require.Zero(t, invalidState)
		return nil
	}))
}
