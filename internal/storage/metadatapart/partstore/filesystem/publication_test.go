package filesystem

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func TestFailedUploadDoesNotPublishPartialPart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	for _, transactional := range []bool{false, true} {
		for _, existing := range []bool{false, true} {
			name := "fresh"
			if existing {
				name = "replacement"
			}
			if transactional {
				name += "-transactional"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				ps, err := New(root)
				require.NoError(t, err)
				require.NoError(t, ps.Start(ctx))
				t.Cleanup(func() { require.NoError(t, ps.Stop(ctx)) })
				id, err := partstore.NewRandomPartId()
				require.NoError(t, err)
				payload := []byte("immutable content")
				var expectedIDs []partstore.PartId
				if existing {
					require.NoError(t, ps.PutPart(ctx, nil, *id, bytes.NewReader(payload)))
					expectedIDs = []partstore.PartId{*id}
				}

				assertPublished := func() {
					t.Helper()
					ids, err := ps.GetPartIds(ctx, nil)
					require.NoError(t, err)
					require.ElementsMatch(t, expectedIDs, ids)
					r, err := ps.GetPart(ctx, nil, *id)
					if !existing {
						require.ErrorIs(t, err, partstore.ErrPartNotFound)
						return
					}
					require.NoError(t, err)
					defer r.Close()
					content, err := io.ReadAll(r)
					require.NoError(t, err)
					require.Equal(t, payload, content)
				}
				uploadErr := errors.New("upload interrupted")
				reader := io.MultiReader(bytes.NewReader(payload[:4]), readerFunc(func([]byte) (int, error) {
					assertPublished()
					return 0, uploadErr
				}))
				if transactional {
					db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, db.Close()) })
					err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
						return ps.PutPart(ctx, tx, *id, reader)
					})
					require.ErrorIs(t, err, uploadErr)
				} else {
					require.ErrorIs(t, ps.PutPart(ctx, nil, *id, reader), uploadErr)
				}
				assertPublished()
				entries, err := os.ReadDir(root)
				require.NoError(t, err)
				require.Len(t, entries, len(expectedIDs), "failed upload must remove its temporary file")
			})
		}
	}
}
