package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/lifecyclereconciler"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
	"time"
)

func TestObjectStorageClass(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should round-trip the storage class through put, head, get and listings"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:       bucketName,
				Key:          key,
				Body:         bytes.NewReader([]byte("cold data")),
				StorageClass: types.StorageClassGlacier,
			})
			assert.Nil(t, err)

			headObjectResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClassGlacier, headObjectResult.StorageClass)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)
			body, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			getObjectResult.Body.Close()
			assert.Equal(t, []byte("cold data"), body)
			assert.Equal(t, types.StorageClassGlacier, getObjectResult.StorageClass)

			listObjectsV2Result, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			assert.Nil(t, err)
			assert.Len(t, listObjectsV2Result.Contents, 1)
			assert.Equal(t, types.ObjectStorageClassGlacier, listObjectsV2Result.Contents[0].StorageClass)

			listObjectsResult, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)
			assert.Len(t, listObjectsResult.Contents, 1)
			assert.Equal(t, types.ObjectStorageClassGlacier, listObjectsResult.Contents[0].StorageClass)

			listObjectVersionsResult, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)
			assert.Len(t, listObjectVersionsResult.Versions, 1)
			assert.Equal(t, types.ObjectVersionStorageClass("GLACIER"), listObjectVersionsResult.Versions[0].StorageClass)
		})

		t.Run("it should default to STANDARD and omit the response header"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("plain data")),
			})
			assert.Nil(t, err)

			// AWS omits x-amz-storage-class for STANDARD objects, which the SDK
			// surfaces as the enum zero value.
			headObjectResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClass(""), headObjectResult.StorageClass)

			listObjectsV2Result, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			assert.Nil(t, err)
			assert.Len(t, listObjectsV2Result.Contents, 1)
			assert.Equal(t, types.ObjectStorageClassStandard, listObjectsV2Result.Contents[0].StorageClass)
		})

		t.Run("it should reject an invalid storage class"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:       bucketName,
				Key:          key,
				Body:         bytes.NewReader([]byte("data")),
				StorageClass: types.StorageClass("BOGUS_CLASS"),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			assert.True(t, errors.As(err, &apiErr))
			assert.Equal(t, "InvalidStorageClass", apiErr.ErrorCode())
		})

		t.Run("it should carry the storage class from CreateMultipartUpload to the completed object"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket:       bucketName,
				Key:          key,
				StorageClass: types.StorageClassStandardIa,
			})
			assert.Nil(t, err)

			listUploadsResult, err := s3Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)
			assert.Len(t, listUploadsResult.Uploads, 1)
			assert.Equal(t, types.StorageClassStandardIa, listUploadsResult.Uploads[0].StorageClass)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Key:        key,
				UploadId:   createResult.UploadId,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader([]byte("multipart data")),
			})
			assert.Nil(t, err)

			listPartsResult, err := s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: createResult.UploadId,
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClassStandardIa, listPartsResult.StorageClass)

			_, err = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: createResult.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{ETag: uploadPartResult.ETag, PartNumber: aws.Int32(1)},
					},
				},
			})
			assert.Nil(t, err)

			headObjectResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClassStandardIa, headObjectResult.StorageClass)
		})

		t.Run("it should apply the storage class of the copy request instead of the source's"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:       bucketName,
				Key:          key,
				Body:         bytes.NewReader([]byte("source data")),
				StorageClass: types.StorageClassGlacier,
			})
			assert.Nil(t, err)

			// Copy with an explicit class: destination gets that class.
			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:       bucketName,
				Key:          aws.String("copy-with-class"),
				CopySource:   aws.String(*bucketName + "/" + *key),
				StorageClass: types.StorageClassOnezoneIa,
			})
			assert.Nil(t, err)
			headObjectResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    aws.String("copy-with-class"),
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClassOnezoneIa, headObjectResult.StorageClass)

			// Copy without a class: destination defaults to STANDARD even though
			// the source is GLACIER (matching AWS).
			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        aws.String("copy-without-class"),
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)
			headObjectResult, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    aws.String("copy-without-class"),
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClass(""), headObjectResult.StorageClass)

			// A self copy that only changes the storage class is allowed.
			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:       bucketName,
				Key:          key,
				CopySource:   aws.String(*bucketName + "/" + *key),
				StorageClass: types.StorageClassStandardIa,
			})
			assert.Nil(t, err)
			headObjectResult, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)
			assert.Equal(t, types.StorageClassStandardIa, headObjectResult.StorageClass)
		})
	})
}

// countPartFiles returns the number of stored part files (32-char hex names)
// directly under a filesystem part store root, or 0 when the root does not
// exist yet.
func countPartFiles(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) == 32 {
			count++
		}
	}
	return count
}

// newNamedPartStoreStorage builds a MetadataPartStorage backed by two
// filesystem part stores: the default store and a "cold" store, with GLACIER
// and DEEP_ARCHIVE routed to the cold store. It returns the storage plus the
// two part store roots so tests can assert where part data physically lives.
func newNamedPartStoreStorage(t *testing.T, addCleanup func(func()), db database.Database) (store storage.Storage, defaultRoot string, coldRoot string) {
	t.Helper()
	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	require.NoError(t, err)
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	require.NoError(t, err)
	partRepository, err := repositoryFactory.NewPartRepository(db)
	require.NoError(t, err)
	tagRepository, err := repositoryFactory.NewTagRepository(db)
	require.NoError(t, err)
	userMetadataRepository, err := repositoryFactory.NewUserMetadataRepository(db)
	require.NoError(t, err)
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	require.NoError(t, err)

	defaultRoot = mustTempDir(addCleanup, "pithos-default-parts-")
	coldRoot = mustTempDir(addCleanup, "pithos-cold-parts-")
	defaultStore, err := filesystemPartStore.New(defaultRoot)
	require.NoError(t, err)
	coldStore, err := filesystemPartStore.New(coldRoot)
	require.NoError(t, err)

	store, err = metadatapart.NewStorageWithNamedPartStores(db, metadataStore, defaultStore,
		map[string]partstore.PartStore{"cold": coldStore},
		map[string]string{"GLACIER": "cold", "DEEP_ARCHIVE": "cold"})
	require.NoError(t, err)
	return store, defaultRoot, coldRoot
}

func TestStorageClassPartStoreRouting(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	ctx := context.Background()
	cleanups := make([]func(), 0, 4)
	addCleanup := func(fn func()) { cleanups = append(cleanups, fn) }
	t.Cleanup(func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	})

	storagePath := mustTempDir(addCleanup, "pithos-test-data-")
	db, dbCleanup, err := setupDatabase(ctx, database.DB_TYPE_SQLITE, storagePath)
	require.NoError(t, err)
	addDatabaseCleanup(addCleanup, db, dbCleanup, "Couldn't close database")

	store, defaultRoot, coldRoot := newNamedPartStoreStorage(t, addCleanup, db)
	require.NoError(t, store.Start(ctx))
	addCleanup(func() { mustNoErr(store.Stop(ctx), "Couldn't stop storage") })

	bucket := storage.MustNewBucketName("routing-test")
	require.NoError(t, store.CreateBucket(ctx, bucket))

	glacierBody := []byte("cold object data")
	_, err = store.PutObject(ctx, bucket, storage.MustNewObjectKey("cold.bin"), nil, ioutils.NewByteReadSeekCloser(glacierBody), nil, &storage.PutObjectOptions{
		StorageClass: aws.String("GLACIER"),
	})
	require.NoError(t, err)

	standardBody := []byte("hot object data")
	_, err = store.PutObject(ctx, bucket, storage.MustNewObjectKey("hot.bin"), nil, ioutils.NewByteReadSeekCloser(standardBody), nil, nil)
	require.NoError(t, err)

	// The GLACIER object's part lives in the cold store; the STANDARD object's
	// part lives in the default store.
	assert.Equal(t, 1, countPartFiles(t, coldRoot), "GLACIER part should be in the cold store")
	assert.Equal(t, 1, countPartFiles(t, defaultRoot), "STANDARD part should be in the default store")

	// Both objects remain readable byte-identically.
	obj, readers, err := store.GetObject(ctx, bucket, storage.MustNewObjectKey("cold.bin"), nil, nil)
	require.NoError(t, err)
	require.Len(t, readers, 1)
	got, err := io.ReadAll(readers[0])
	require.NoError(t, err)
	readers[0].Close()
	assert.Equal(t, glacierBody, got)
	assert.Equal(t, "GLACIER", storage.EffectiveStorageClass(obj.StorageClass))
}

func TestStorageClassLifecycleTransitionEndToEnd(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	ctx := context.Background()
	cleanups := make([]func(), 0, 4)
	addCleanup := func(fn func()) { cleanups = append(cleanups, fn) }
	t.Cleanup(func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	})

	storagePath := mustTempDir(addCleanup, "pithos-test-data-")
	db, dbCleanup, err := setupDatabase(ctx, database.DB_TYPE_SQLITE, storagePath)
	require.NoError(t, err)
	addDatabaseCleanup(addCleanup, db, dbCleanup, "Couldn't close database")

	innerStore, defaultRoot, coldRoot := newNamedPartStoreStorage(t, addCleanup, db)

	// Run the reconciler frequently and pretend the sweep runs 10 days ahead so
	// a 1-day transition is due immediately.
	store := lifecyclereconciler.NewStorageMiddleware(innerStore,
		lifecyclereconciler.WithReconcileInterval(100*time.Millisecond),
		lifecyclereconciler.WithNow(func() time.Time { return time.Now().UTC().AddDate(0, 0, 10) }))
	require.NoError(t, store.Start(ctx))
	addCleanup(func() { mustNoErr(store.Stop(ctx), "Couldn't stop storage") })

	bucket := storage.MustNewBucketName("transition-test")
	require.NoError(t, store.CreateBucket(ctx, bucket))

	body := []byte("data to be tiered to cold storage")
	_, err = store.PutObject(ctx, bucket, storage.MustNewObjectKey("cold/a.bin"), nil, ioutils.NewByteReadSeekCloser(body), nil, nil)
	require.NoError(t, err)

	// Initially STANDARD: the part is in the default store.
	assert.Equal(t, 1, countPartFiles(t, defaultRoot))
	assert.Equal(t, 0, countPartFiles(t, coldRoot))

	require.NoError(t, store.PutBucketLifecycleConfiguration(ctx, bucket, &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{
			{
				ID:          aws.String("tier-cold"),
				Status:      storage.LifecycleRuleStatusEnabled,
				Filter:      &storage.LifecycleFilter{Prefix: aws.String("cold/")},
				Transitions: []storage.LifecycleTransition{{Days: aws.Int32(1), StorageClass: "GLACIER"}},
			},
		},
	}))

	// The reconciler transitions the object: class flips to GLACIER, the part is
	// relocated to the cold store, and the source part is deleted inline as part
	// of the same transaction (no dependency on the background GC).
	require.Eventually(t, func() bool {
		obj, err := store.HeadObject(ctx, bucket, storage.MustNewObjectKey("cold/a.bin"), nil)
		if err != nil {
			return false
		}
		return storage.EffectiveStorageClass(obj.StorageClass) == "GLACIER" &&
			countPartFiles(t, coldRoot) == 1 &&
			countPartFiles(t, defaultRoot) == 0
	}, 15*time.Second, 100*time.Millisecond, "object should transition to GLACIER and relocate its part")

	// The object is still readable byte-identically after the transition.
	_, readers, err := store.GetObject(ctx, bucket, storage.MustNewObjectKey("cold/a.bin"), nil, nil)
	require.NoError(t, err)
	require.Len(t, readers, 1)
	got, err := io.ReadAll(readers[0])
	require.NoError(t, err)
	readers[0].Close()
	assert.Equal(t, body, got)
}
