package gc

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestOldPartSurvivesDereferenceUntilRetention(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var env *gcTestEnv
			if backend == "postgres" {
				env = newPostgresGCTestEnv(t)
			} else {
				env = newSqliteGCTestEnv(t)
			}
			store, err := filesystem.New(t.TempDir())
			require.NoError(t, err)
			require.NoError(t, store.Start(ctx))
			t.Cleanup(func() { require.NoError(t, store.Stop(ctx)) })
			env.collector.partStores, err = partstore.NewNamedPartStores(store, nil, nil)
			require.NoError(t, err)
			old := ulid.MustNew(ulid.Timestamp(time.Now().Add(-24*time.Hour)), rand.Reader)
			id := *partstore.MustNewPartIdFromString(old.String())
			require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
				require.NoError(t, store.PutPart(ctx, tx, id, bytes.NewBufferString("old data")))
				require.NoError(t, env.registryRepo.RegisterParts(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: id, Delta: 1}}))
				removed, err := env.registryRepo.RemoveReferences(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: id, Delta: 1}})
				require.Empty(t, removed)
				return err
			}))
			assertPresent := func(present bool) {
				require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
					ids, err := store.GetPartIds(ctx, tx)
					if present {
						require.Contains(t, ids, id)
					} else {
						require.NotContains(t, ids, id)
					}
					return err
				}))
			}
			require.NoError(t, env.collector.runGC())
			assertPresent(true)
			require.NoError(t, env.collector.runGC())
			assertPresent(true)
			// Advance persisted dereference time, not part creation time; no sleeps.
			require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
				_, err := tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET unreferenced_at = $1 WHERE part_id = $2", time.Now().UTC().Add(-time.Hour), id.String())
				return err
			}))
			require.NoError(t, env.collector.runGC())
			assertPresent(false)
		})
	}
}
