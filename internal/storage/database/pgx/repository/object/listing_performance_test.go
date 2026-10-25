package pgx

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/jdillenkofer/pithos/internal/storage"
	databasebackend "github.com/jdillenkofer/pithos/internal/storage/database/pgx"
	bucketrepository "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"testing"
)

func newListingPerformanceRepository(t testing.TB) (*sql.Tx, object.Repository) {
	t.Helper()
	ctx := context.Background()
	container, err := postgres.Run(ctx, testutils.PostgresImage, postgres.WithUsername("postgres"), postgres.WithPassword("postgres"), postgres.WithDatabase("postgres"), postgres.BasicWaitStrategies())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })
	url, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db, err := databasebackend.OpenDatabase(url, nil)
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
	_, err = tx.ExecContext(t.Context(), "ANALYZE objects")
	require.NoError(t, err)
	return tx, repo
}

func TestListingBatchUsesIndexedSeek(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)
	tx, repo := newListingPerformanceRepository(t)
	for _, minimum := range []string{"", "a0"} {
		t.Run("minimum="+minimum, func(t *testing.T) {
			stmt := findObjectsFromKeyStmt
			if minimum == "" {
				stmt = findObjectsAfterKeyStmt
			}
			var encoded []byte
			require.NoError(t, tx.QueryRowContext(t.Context(), "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+stmt, "bucket", minimum, object.UploadStatusCompleted, "", 64).Scan(&encoded))
			t.Logf("minimum=%q plan=%s", minimum, encoded)
			var plans []struct{ Plan map[string]any }
			require.NoError(t, json.Unmarshal(encoded, &plans))
			var inspect func(map[string]any)
			indexFound := false
			inspect = func(node map[string]any) {
				require.NotEqual(t, "Sort", node["Node Type"], "a seek must not sort all matching keys")
				require.LessOrEqual(t, node["Actual Rows"].(float64), float64(64))
				if node["Node Type"] == "Index Scan" || node["Node Type"] == "Index Only Scan" {
					indexFound = true
					require.Equal(t, "objects_completed_listing_idx", node["Index Name"])
					if minimum != "" {
						require.Contains(t, node["Index Cond"].(string), minimum)
					}
				}
				if removed, ok := node["Rows Removed by Filter"].(float64); ok {
					require.Zero(t, removed)
				}
				if children, ok := node["Plans"].([]any); ok {
					for _, child := range children {
						inspect(child.(map[string]any))
					}
				}
			}
			inspect(plans[0].Plan)
			require.True(t, indexFound)
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
