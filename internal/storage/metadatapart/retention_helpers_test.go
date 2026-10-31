package metadatapart

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/stretchr/testify/require"
)

// Deletion removes metadata immediately, but physical content and zero-reference
// registry rows must survive until GC retention expires. Duplicates of
// deduplicated writes were never referenced and are exempt from retention.
func assertRetainedUnreferencedParts(t *testing.T, st *metadataPartStorage) {
	t.Helper()
	require.NotEmpty(t, physicalPartIDs(t, st))
	require.NoError(t, database.WithTx(context.Background(), st.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var live int
		require.NoError(t, tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM parts").Scan(&live))
		require.Zero(t, live)
		rows, err := tx.SqlTx().QueryContext(ctx, "SELECT ref_count, unreferenced_at FROM part_registry")
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var refCount int64
			var unreferencedAt sql.NullTime
			require.NoError(t, rows.Scan(&refCount, &unreferencedAt))
			require.Zero(t, refCount)
			// Never-referenced parts start out expired; others await a GC observation.
			if unreferencedAt.Valid {
				require.True(t, unreferencedAt.Time.Equal(time.Unix(0, 0)), "unexpected GC observation at %v", unreferencedAt.Time)
			}
		}
		return rows.Err()
	}))
}
