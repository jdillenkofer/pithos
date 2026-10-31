package gc

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

type retryDeletionStore struct {
	partstore.PartStore
	fail bool
}

func (s *retryDeletionStore) Capabilities() partstore.Capabilities {
	return partstore.CapabilitiesOf(s.PartStore)
}
func (s *retryDeletionStore) DeletePart(ctx context.Context, tx database.Tx, id partstore.PartId) error {
	if s.fail {
		s.fail = false
		return errors.New("injected deletion failure")
	}
	return s.PartStore.DeletePart(ctx, tx, id)
}

func TestOldPartSurvivesDereferenceUntilRetention(t *testing.T) {
	testutils.SkipIfIntegration(t)
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
			retryStore := &retryDeletionStore{PartStore: store}
			env.collector.partStores, err = partstore.NewNamedPartStores(retryStore, nil, nil)
			require.NoError(t, err)
			old := ulid.MustNew(ulid.Timestamp(time.Now().Add(-24*time.Hour)), rand.Reader)
			id := *partstore.MustNewPartIdFromString(old.String())
			require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
				require.NoError(t, store.PutPart(ctx, tx, id, bytes.NewBufferString("old data")))
				require.NoError(t, env.registryRepo.RegisterParts(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: id, Delta: 1}}))
				return env.registryRepo.RemoveReferences(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: id, Delta: 1}})
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
			retryStore.fail = true
			require.NoError(t, env.collector.runGC())
			assertPresent(true) // Condemnation committed, external delete failed.
			require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
				ok, err := env.registryRepo.TryAddReferences(ctx, tx.SqlTx(), []partregistry.Ref{{PartId: id, Delta: 1}})
				require.NoError(t, err)
				require.False(t, ok, "a condemned row must not resurrect its id")
				return nil
			}))
			// The condemned row survives the failed delete, so the next sweep
			// retries it without starting a new retention window.
			require.NoError(t, env.collector.runGC())
			assertPresent(false)
			require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
				rows, err := env.registryRepo.FindAllEntities(ctx, tx.SqlTx())
				require.Empty(t, rows)
				return err
			}))
		})
	}
}
