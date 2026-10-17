package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	databasebackend "github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	bucketrepository "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/bucket"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func newListingPerformanceRepository(t testing.TB) (*sql.Tx, object.Repository) {
	t.Helper()
	db, err := databasebackend.OpenDatabase(filepath.Join(t.TempDir(), "listing.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	controller, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = controller.Rollback(context.Background()) })
	tx := controller.SqlTx()
	buckets, err := bucketrepository.NewRepository()
	require.NoError(t, err)
	name := storage.MustNewBucketName("bucket")
	require.NoError(t, buckets.SaveBucket(t.Context(), tx, &bucket.Entity{Name: name, OwnerAccountID: "account"}))
	repo, err := NewRepository()
	require.NoError(t, err)
	for _, shape := range []struct {
		prefix string
		count  int
	}{{"a/", 10000}, {"z", 2000}} {
		for i := range shape.count {
			entity := object.Entity{BucketName: name, Key: storage.MustNewObjectKey(fmt.Sprintf("%s%06d", shape.prefix, i)), UploadStatus: object.UploadStatusCompleted, IsLatest: true}
			require.NoError(t, repo.SaveObject(t.Context(), tx, &entity))
		}
	}

	return tx, repo
}

func TestListingBatchUsesIndexedSeek(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tx, repo := newListingPerformanceRepository(t)
	for _, minimum := range []string{"", "a0"} {
		t.Run("minimum="+minimum, func(t *testing.T) {
			stmt := findObjectsFromKeyStmt
			if minimum == "" {
				stmt = findObjectsAfterKeyStmt
			}
			rows, err := tx.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+stmt, "bucket", minimum, object.UploadStatusCompleted, "", 64)
			require.NoError(t, err)
			defer rows.Close()
			indexFound := false
			for rows.Next() {
				var id, parent, unused int
				var detail string
				require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
				t.Logf("minimum=%q plan=%s", minimum, detail)
				require.NotContains(t, detail, "TEMP B-TREE")
				require.NotContains(t, detail, "SCAN objects")
				if strings.Contains(detail, "SEARCH objects USING INDEX") && strings.Contains(detail, "key>?") {
					indexFound = true
				}
			}
			require.NoError(t, rows.Err())
			require.True(t, indexFound, "the lower key bound must be an index seek")
			batch, err := repo.FindObjectsFromKey(t.Context(), tx, storage.MustNewBucketName("bucket"), "", "", minimum, 64)
			require.NoError(t, err)
			require.Len(t, batch, 64)
			expected := "a/000000"
			if minimum != "" {
				expected = "z000000"
			}
			require.Equal(t, expected, batch[0].Key.String())
		})
	}
}

// Compare the old single-row strategy with bounded batches against a real DB.
// Run with -bench=BenchmarkListingSeeks -benchmem; PostgreSQL needs Docker.
func BenchmarkListingSeeks(b *testing.B) {
	tx, repo := newListingPerformanceRepository(b)
	name := storage.MustNewBucketName("bucket")
	for _, batchSize := range []int32{1, 64} {
		b.Run(fmt.Sprintf("direct/batch=%d", batchSize), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				marker := ""
				for fetched := 0; fetched < 1001; {
					batch, err := repo.FindObjectsFromKey(b.Context(), tx, name, "z", marker, "z", min(batchSize, int32(1001-fetched)))
					if err != nil {
						b.Fatal(err)
					}
					if len(batch) == 0 {
						b.Fatal("unexpected end of listing")
					}
					fetched += len(batch)
					marker = batch[len(batch)-1].Key.String()
				}
			}
			b.ReportMetric(float64((1001+batchSize-1)/batchSize), "queries/op")
		})
	}
	b.Run("skip-10000-descendants", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			for _, minimum := range []string{"", "a0"} {
				batch, err := repo.FindObjectsFromKey(b.Context(), tx, name, "", "", minimum, 2)
				if err != nil {
					b.Fatal(err)
				}
				if len(batch) != 2 {
					b.Fatal("unexpected batch size")
				}
			}
		}
		b.ReportMetric(2, "queries/op")
	})
}
