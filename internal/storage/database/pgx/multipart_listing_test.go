package pgx

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	sqlmetadatastore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestMultipartListingUsesBytewiseOrder(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := t.Context()
	container, err := setupPostgresContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(context.Background())) })
	dbURL, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db, err := OpenDatabase(dbURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// Force a locale whose ordering disagrees with Go's bytewise comparisons,
	// even if the container's default database collation is C.
	_, err = db.ExecContext(ctx, `CREATE COLLATION listing_test_english (provider = icu, locale = 'en-US')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `ALTER TABLE objects ALTER COLUMN key TYPE TEXT COLLATE listing_test_english`)
	require.NoError(t, err)
	var localeSortsLowercaseFirst bool
	err = db.QueryRowContext(ctx, `SELECT 'a' COLLATE listing_test_english < 'B/x'`).Scan(&localeSortsLowercaseFirst)
	require.NoError(t, err)
	require.True(t, localeSortsLowercaseFirst, "the test must exercise a non-bytewise collation")

	bucketRepository, err := repositoryfactory.NewBucketRepository(db)
	require.NoError(t, err)
	objectRepository, err := repositoryfactory.NewObjectRepository(db)
	require.NoError(t, err)
	partRepository, err := repositoryfactory.NewPartRepository(db)
	require.NoError(t, err)
	tagRepository, err := repositoryfactory.NewTagRepository(db)
	require.NoError(t, err)
	userMetadataRepository, err := repositoryfactory.NewUserMetadataRepository(db)
	require.NoError(t, err)
	store, err := sqlmetadatastore.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	require.NoError(t, err)

	tx, err := db.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	bucketName := storage.MustNewBucketName("bucket")
	require.NoError(t, bucketRepository.SaveBucket(ctx, tx, &bucket.Entity{Name: bucketName, OwnerAccountID: "account"}))

	var uploads []metadatastore.Upload
	for _, key := range []string{"a", "B/x", "B/y", "Z", "ä/x", "z", "a"} {
		objectKey := storage.MustNewObjectKey(key)
		created, err := store.CreateMultipartUpload(ctx, tx, bucketName, objectKey, nil, nil, nil)
		require.NoError(t, err)
		uploads = append(uploads, metadatastore.Upload{Key: objectKey, UploadId: created.UploadId})
	}
	slices.SortFunc(uploads, func(a, b metadatastore.Upload) int {
		if order := cmp.Compare(a.Key.String(), b.Key.String()); order != 0 {
			return order
		}
		return cmp.Compare(a.UploadId.String(), b.UploadId.String())
	})
	expectedIDs := make([]storage.UploadId, len(uploads))
	expectedDirectIDs := []storage.UploadId{}
	for i, upload := range uploads {
		expectedIDs[i] = upload.UploadId
		if !strings.Contains(upload.Key.String(), "/") {
			expectedDirectIDs = append(expectedDirectIDs, upload.UploadId)
		}
	}

	t.Run("repository markers", func(t *testing.T) {
		for markerIndex := -1; markerIndex < len(uploads); markerIndex++ {
			keyMarker, uploadIDMarker := "", ""
			if markerIndex >= 0 {
				keyMarker = uploads[markerIndex].Key.String()
				uploadIDMarker = uploads[markerIndex].UploadId.String()
			}
			remaining, err := objectRepository.FindUploadsFromKey(ctx, tx, bucketName, "", keyMarker, uploadIDMarker, "", math.MaxInt32)
			require.NoError(t, err)
			actualIDs := make([]storage.UploadId, len(remaining))
			for i, upload := range remaining {
				actualIDs[i] = *upload.UploadId
			}
			require.Equal(t, expectedIDs[markerIndex+1:], actualIDs)
		}

		// Omitting the upload-ID marker excludes every upload at the marker key.
		remaining, err := objectRepository.FindUploadsFromKey(ctx, tx, bucketName, "", "a", "", "", math.MaxInt32)
		require.NoError(t, err)
		require.Len(t, remaining, 2)
		require.Equal(t, "z", remaining[0].Key.String())
		require.Equal(t, "ä/x", remaining[1].Key.String())
	})

	for _, delimiter := range []string{"", "/"} {
		for _, pageSize := range []int32{1, 2, 3, 1000} {
			t.Run(fmt.Sprintf("delimiter=%q/page-size=%d", delimiter, pageSize), func(t *testing.T) {
				opts := metadatastore.ListMultipartUploadsOptions{Delimiter: &delimiter, MaxUploads: pageSize}
				actualIDs := []storage.UploadId{}
				actualPrefixes := []string{}
				for page := 0; ; page++ {
					require.Less(t, page, len(uploads)+1, "pagination must make progress")
					result, err := store.ListMultipartUploads(ctx, tx, bucketName, opts)
					require.NoError(t, err)
					count := len(result.Uploads) + len(result.CommonPrefixes)
					require.LessOrEqual(t, count, int(pageSize))
					for _, upload := range result.Uploads {
						actualIDs = append(actualIDs, upload.UploadId)
					}
					actualPrefixes = append(actualPrefixes, result.CommonPrefixes...)
					if !result.IsTruncated {
						break
					}
					require.Equal(t, int(pageSize), count)
					opts.KeyMarker = &result.NextKeyMarker
					opts.UploadIdMarker = &result.NextUploadIdMarker
				}
				if delimiter == "" {
					require.Equal(t, expectedIDs, actualIDs)
					require.Empty(t, actualPrefixes)
				} else {
					require.Equal(t, expectedDirectIDs, actualIDs)
					require.Equal(t, []string{"B/", "ä/"}, actualPrefixes)
				}
			})
		}
	}
}
