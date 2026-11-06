package pgx

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	bucketrepository "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestGroupLiveObjectsByBucketExcludesHistoryAndDeleteMarkers(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)
	tx, repo := newListingPerformanceRepository(t)
	buckets, err := bucketrepository.NewRepository()
	require.NoError(t, err)
	empty := storage.MustNewBucketName("empty")
	require.NoError(t, buckets.SaveBucket(t.Context(), tx, &bucket.Entity{Name: empty, OwnerAccountID: "account"}))
	name := storage.MustNewBucketName("bucket")
	for _, entity := range []object.Entity{
		{BucketName: name, Key: storage.MustNewObjectKey("sized"), UploadStatus: object.UploadStatusCompleted, IsLatest: true, Size: 7},
		{BucketName: name, Key: storage.MustNewObjectKey("historical"), UploadStatus: object.UploadStatusCompleted, IsLatest: false, Size: 100},
		{BucketName: name, Key: storage.MustNewObjectKey("marker"), UploadStatus: object.UploadStatusCompleted, IsLatest: true, IsDeleteMarker: true},
		{BucketName: name, Key: storage.MustNewObjectKey("pending"), UploadStatus: object.UploadStatusPending, IsLatest: true, Size: 1000},
	} {
		require.NoError(t, repo.SaveObject(t.Context(), tx, &entity))
	}

	aggregates, err := repo.GroupLiveObjectsByBucket(t.Context(), tx)
	require.NoError(t, err)
	require.ElementsMatch(t, []object.BucketAggregate{
		{BucketName: "bucket", Count: 12001, Size: 7},
		{BucketName: "empty", Count: 0, Size: 0},
	}, aggregates)
}
