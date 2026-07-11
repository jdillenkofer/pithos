package metadatapart

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	pgxdatabase "github.com/jdillenkofer/pithos/internal/storage/database/pgx"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	sqlPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestEvaluateCopySourceConditionsMatchesS3IfMatchAndUnmodifiedSincePrecedence(t *testing.T) {
	testutils.SkipIfIntegration(t)

	etag := "\"etag\""
	lastModified := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	object := &metadatastore.Object{
		ETag:         etag,
		LastModified: lastModified,
	}
	beforeLastModified := lastModified.Add(-time.Hour)
	afterLastModified := lastModified.Add(time.Hour)

	err := evaluateCopySourceConditions(storage.CopySourceConditions{
		IfMatch:           ptrutils.ToPtr(etag),
		IfUnmodifiedSince: &beforeLastModified,
	}, object)
	require.NoError(t, err)

	err = evaluateCopySourceConditions(storage.CopySourceConditions{
		IfMatch:           ptrutils.ToPtr("\"different\""),
		IfUnmodifiedSince: &afterLastModified,
	}, object)
	require.ErrorIs(t, err, storage.ErrPreconditionFailed)

	err = evaluateCopySourceConditions(storage.CopySourceConditions{
		IfUnmodifiedSince: &beforeLastModified,
	}, object)
	require.ErrorIs(t, err, storage.ErrPreconditionFailed)
}

func TestMetadataPartStorageWithSql(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		slog.Error("Couldn't open database")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()

	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartContentRepository: %s", err))
		os.Exit(1)
	}
	partStore, err := sqlPartStore.New(db, partContentRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlPartStore: %s", err))
		os.Exit(1)
	}

	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BucketRepository: %s", err))
		os.Exit(1)
	}
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ObjectRepository: %s", err))
		os.Exit(1)
	}
	partRepository, err := repositoryFactory.NewPartRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartRepository: %s", err))
		os.Exit(1)
	}
	tagRepository, err := repositoryFactory.NewTagRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create TagRepository: %s", err))
		os.Exit(1)
	}
	userMetadataRepository, err := repositoryFactory.NewUserMetadataRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create UserMetadataRepository: %s", err))
		os.Exit(1)
	}
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlMetadataStore: %s", err))
		os.Exit(1)
	}

	metadataPartStorage, err := NewStorage(db, metadataStore, partStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create MetadataPartStorage: %s", err))
		os.Exit(1)
	}
	content := []byte("MetadataPartStorage")
	err = storage.Tester(metadataPartStorage, []storage.BucketName{storage.MustNewBucketName("bucket")}, content)
	assert.Nil(t, err)
}

func TestMetadataPartStorageWithFilesystem(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		slog.Error("Couldn't open database")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()

	partStore, err := filesystemPartStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemPartStore: %s", err))
		os.Exit(1)
	}

	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BucketRepository: %s", err))
		os.Exit(1)
	}
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ObjectRepository: %s", err))
		os.Exit(1)
	}
	partRepository, err := repositoryFactory.NewPartRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartRepository: %s", err))
		os.Exit(1)
	}
	tagRepository, err := repositoryFactory.NewTagRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create TagRepository: %s", err))
		os.Exit(1)
	}
	userMetadataRepository, err := repositoryFactory.NewUserMetadataRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create UserMetadataRepository: %s", err))
		os.Exit(1)
	}
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlMetadataStore: %s", err))
		os.Exit(1)
	}

	metadataPartStorage, err := NewStorage(db, metadataStore, partStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create MetadataPartStorage: %s", err))
		os.Exit(1)
	}
	content := []byte("MetadataPartStorage")
	err = storage.Tester(metadataPartStorage, []storage.BucketName{storage.MustNewBucketName("bucket")}, content)
	assert.Nil(t, err)
}

// newTestStorage creates a fresh MetadataPartStorage backed by a temporary SQLite DB and SQL part store.
// Returns the raw *metadataPartStorage (for access to internal methods) and a cleanup function.
func newTestStorage(t *testing.T) (*metadataPartStorage, func()) {
	t.Helper()
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	require.NoError(t, err)

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	require.NoError(t, err)

	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	require.NoError(t, err)
	partStore, err := sqlPartStore.New(db, partContentRepository)
	require.NoError(t, err)
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
	metaStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	require.NoError(t, err)
	st, err := NewStorage(db, metaStore, partStore)
	require.NoError(t, err)
	mps := st.(*metadataPartStorage)

	ctx := context.Background()
	require.NoError(t, mps.Start(ctx))

	cleanup := func() {
		mps.Stop(ctx)
		db.Close()
		os.RemoveAll(storagePath)
	}
	return mps, cleanup
}

func newPostgresTestStorage(t *testing.T) (*metadataPartStorage, func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := t.Context()
	container, err := postgres.Run(ctx, "postgres:18.4-alpine3.24@sha256:1b1689b20d16a014a3d195653381cf2caa75a41a92d93b255a9d6ea29fd353aa",
		postgres.WithUsername("postgres"), postgres.WithPassword("postgres"), postgres.WithDatabase("postgres"), postgres.BasicWaitStrategies())
	require.NoError(t, err)
	dbURL, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db, err := pgxdatabase.OpenDatabase(dbURL, nil)
	require.NoError(t, err)

	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	require.NoError(t, err)
	partStore, err := sqlPartStore.New(db, partContentRepository)
	require.NoError(t, err)
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
	metaStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	require.NoError(t, err)
	st, err := NewStorage(db, metaStore, partStore)
	require.NoError(t, err)
	mps := st.(*metadataPartStorage)
	require.NoError(t, mps.Start(ctx))
	cleanup := func() {
		require.NoError(t, mps.Stop(ctx))
		require.NoError(t, db.Close())
		require.NoError(t, container.Terminate(ctx))
	}
	return mps, cleanup
}

func readObjectContent(t *testing.T, st *metadataPartStorage, bucket storage.BucketName, key storage.ObjectKey, versionID *string) string {
	t.Helper()

	var opts *storage.GetObjectOptions
	if versionID != nil {
		opts = &storage.GetObjectOptions{VersionID: versionID}
	}

	_, readers, err := st.GetObject(context.Background(), bucket, key, nil, opts)
	require.NoError(t, err)
	require.Len(t, readers, 1)
	defer readers[0].Close()

	content, err := io.ReadAll(readers[0])
	require.NoError(t, err)
	return string(content)
}

func physicalPartIDs(t *testing.T, st *metadataPartStorage) []partstore.PartId {
	t.Helper()
	ctx := context.Background()
	var ids []partstore.PartId
	require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: true}, func(_ context.Context, tx database.Tx) error {
		var err error
		ids, err = st.partStores.Default().GetPartIds(ctx, tx)
		return err
	}))
	return ids
}

func versionIDsForKey(t *testing.T, st *metadataPartStorage, bucket storage.BucketName, key storage.ObjectKey) (latestVersionID string, olderVersionIDs []string) {
	t.Helper()

	versions, err := st.ListObjectVersions(context.Background(), bucket, storage.ListObjectVersionsOptions{MaxKeys: 1000})
	require.NoError(t, err)

	for _, version := range versions.Versions {
		if !version.Key.Equals(key) || version.IsDeleteMarker {
			continue
		}
		if version.IsLatest {
			latestVersionID = version.VersionID
			continue
		}
		olderVersionIDs = append(olderVersionIDs, version.VersionID)
	}

	require.NotEmpty(t, latestVersionID)
	return latestVersionID, olderVersionIDs
}

func enableVersioning(t *testing.T, st *metadataPartStorage, bucket storage.BucketName) {
	t.Helper()
	status := storage.BucketVersioningStatusEnabled
	require.NoError(t, st.PutBucketVersioningConfiguration(context.Background(), bucket, &storage.BucketVersioningConfiguration{Status: &status}))
}

func TestBucketNotificationConfigurationRoundTrips(t *testing.T) {
	testutils.SkipIfIntegration(t)

	st, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	emptyConfig, err := st.GetBucketNotificationConfiguration(ctx, bucket)
	require.NoError(t, err)
	require.Empty(t, emptyConfig.QueueConfigurations)
	require.False(t, emptyConfig.EventBridgeEnabled)

	config := &storage.BucketNotificationConfiguration{
		QueueConfigurations: []storage.NotificationConfigurationRule{{
			ID:              ptrutils.ToPtr("all-events"),
			DestinationType: storage.NotificationDestinationQueue,
			DestinationARN:  "arn:aws:sqs:eu-central-1:000000000000:pithos-events",
			Events:          []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"},
			FilterRules:     []storage.NotificationFilterRule{{Name: "prefix", Value: "images/"}},
		}},
		EventBridgeEnabled: true,
	}

	require.NoError(t, st.PutBucketNotificationConfiguration(ctx, bucket, config))
	loaded, err := st.GetBucketNotificationConfiguration(ctx, bucket)
	require.NoError(t, err)
	require.True(t, loaded.EventBridgeEnabled)
	require.Len(t, loaded.QueueConfigurations, 1)
	require.Equal(t, "all-events", *loaded.QueueConfigurations[0].ID)
	require.Equal(t, []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"}, loaded.QueueConfigurations[0].Events)
	require.Equal(t, []storage.NotificationFilterRule{{Name: "prefix", Value: "images/"}}, loaded.QueueConfigurations[0].FilterRules)

	require.NoError(t, st.PutBucketNotificationConfiguration(ctx, bucket, &storage.BucketNotificationConfiguration{}))
	loaded, err = st.GetBucketNotificationConfiguration(ctx, bucket)
	require.NoError(t, err)
	require.Empty(t, loaded.QueueConfigurations)
	require.False(t, loaded.EventBridgeEnabled)
}

func TestCopyObjectCopiesExplicitSourceVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	srcKey := storage.MustNewObjectKey("src")
	dstKey := storage.MustNewObjectKey("dst")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	enableVersioning(t, st, bucket)

	_, err := st.PutObject(ctx, bucket, srcKey, nil, bytes.NewReader([]byte("old")), nil, nil)
	require.NoError(t, err)
	_, err = st.PutObject(ctx, bucket, srcKey, nil, bytes.NewReader([]byte("new")), nil, nil)
	require.NoError(t, err)
	_, olderVersions := versionIDsForKey(t, st, bucket, srcKey)
	require.Len(t, olderVersions, 1)

	copyResult, err := st.CopyObject(ctx, bucket, srcKey, bucket, dstKey, &storage.CopyObjectOptions{SourceVersionID: &olderVersions[0]})
	require.NoError(t, err)
	require.NotNil(t, copyResult.SourceVersionID)
	assert.Equal(t, olderVersions[0], *copyResult.SourceVersionID)

	assert.Equal(t, "old", readObjectContent(t, st, bucket, dstKey, nil))
}

func TestUploadPartCopyCopiesExplicitSourceVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	srcKey := storage.MustNewObjectKey("src")
	dstKey := storage.MustNewObjectKey("dst")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	enableVersioning(t, st, bucket)

	_, err := st.PutObject(ctx, bucket, srcKey, nil, bytes.NewReader([]byte("old")), nil, nil)
	require.NoError(t, err)
	_, err = st.PutObject(ctx, bucket, srcKey, nil, bytes.NewReader([]byte("new")), nil, nil)
	require.NoError(t, err)
	_, olderVersions := versionIDsForKey(t, st, bucket, srcKey)
	require.Len(t, olderVersions, 1)

	createResult, err := st.CreateMultipartUpload(ctx, bucket, dstKey, nil, nil, nil)
	require.NoError(t, err)
	copyResult, err := st.UploadPartCopy(ctx, bucket, srcKey, bucket, dstKey, createResult.UploadId, 1, &storage.UploadPartCopyOptions{SourceVersionID: &olderVersions[0]})
	require.NoError(t, err)
	require.NotNil(t, copyResult.SourceVersionID)
	assert.Equal(t, olderVersions[0], *copyResult.SourceVersionID)
	_, err = st.CompleteMultipartUpload(ctx, bucket, dstKey, createResult.UploadId, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, "old", readObjectContent(t, st, bucket, dstKey, nil))
}

func TestCopyObjectSharesPartsInSameStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	srcKey := storage.MustNewObjectKey("src")
	dstKey := storage.MustNewObjectKey("dst")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err := st.PutObject(ctx, bucket, srcKey, nil, bytes.NewReader([]byte("shared")), nil, nil)
	require.NoError(t, err)
	require.Len(t, physicalPartIDs(t, st), 1)

	_, err = st.CopyObject(ctx, bucket, srcKey, bucket, dstKey, nil)
	require.NoError(t, err)
	require.Len(t, physicalPartIDs(t, st), 1)

	_, err = st.DeleteObject(ctx, bucket, srcKey, nil)
	require.NoError(t, err)
	assert.Equal(t, "shared", readObjectContent(t, st, bucket, dstKey, nil))
	require.Len(t, physicalPartIDs(t, st), 1)
	_, err = st.DeleteObject(ctx, bucket, dstKey, nil)
	require.NoError(t, err)
	assert.Empty(t, physicalPartIDs(t, st))
}

func TestIdenticalPutObjectsDeduplicateContent(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	firstKey := storage.MustNewObjectKey("first")
	secondKey := storage.MustNewObjectKey("second")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	content := []byte("identical content")
	_, err := st.PutObject(ctx, bucket, firstKey, nil, bytes.NewReader(content), nil, nil)
	require.NoError(t, err)
	_, err = st.PutObject(ctx, bucket, secondKey, nil, bytes.NewReader(content), nil, nil)
	require.NoError(t, err)
	require.Len(t, physicalPartIDs(t, st), 1)

	_, err = st.DeleteObject(ctx, bucket, firstKey, nil)
	require.NoError(t, err)
	assert.Equal(t, string(content), readObjectContent(t, st, bucket, secondKey, nil))
	require.Len(t, physicalPartIDs(t, st), 1)
	_, err = st.DeleteObject(ctx, bucket, secondKey, nil)
	require.NoError(t, err)
	assert.Empty(t, physicalPartIDs(t, st))
}

func TestTransitionWithinSameStoreKeepsContent(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("object")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("relabeled")), nil, nil)
	require.NoError(t, err)

	// A single-store setup maps every class to the default store, so the
	// transition only relabels the object and must share the existing parts.
	require.NoError(t, st.TransitionObjectStorageClass(ctx, bucket, key, "GLACIER", nil))

	assert.Equal(t, "relabeled", readObjectContent(t, st, bucket, key, nil))
	require.Len(t, physicalPartIDs(t, st), 1)
	_, err = st.DeleteObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	assert.Empty(t, physicalPartIDs(t, st))
}

func TestTransitionWithinSameStoreKeepsSharedPartLive(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	srcKey := storage.MustNewObjectKey("src")
	dstKey := storage.MustNewObjectKey("dst")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err := st.PutObject(ctx, bucket, srcKey, nil, bytes.NewReader([]byte("shared")), nil, nil)
	require.NoError(t, err)
	_, err = st.CopyObject(ctx, bucket, srcKey, bucket, dstKey, nil)
	require.NoError(t, err)
	require.Len(t, physicalPartIDs(t, st), 1)

	// The transitioned object shares its part with the copy; relabeling must
	// bump the shared reference instead of re-registering the part.
	require.NoError(t, st.TransitionObjectStorageClass(ctx, bucket, srcKey, "GLACIER", nil))

	assert.Equal(t, "shared", readObjectContent(t, st, bucket, srcKey, nil))
	_, err = st.DeleteObject(ctx, bucket, srcKey, nil)
	require.NoError(t, err)
	assert.Equal(t, "shared", readObjectContent(t, st, bucket, dstKey, nil))
	require.Len(t, physicalPartIDs(t, st), 1)
	_, err = st.DeleteObject(ctx, bucket, dstKey, nil)
	require.NoError(t, err)
	assert.Empty(t, physicalPartIDs(t, st))
}

func TestDedupIndexChecksumMismatchDoesNotShare(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	content := []byte("collision guard")
	_, err := st.PutObject(ctx, bucket, storage.MustNewObjectKey("first"), nil, bytes.NewReader(content), nil, nil)
	require.NoError(t, err)
	require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		_, err := tx.SqlTx().ExecContext(ctx, "UPDATE part_dedup_index SET checksum_crc32 = 'mismatch'")
		return err
	}))
	_, err = st.PutObject(ctx, bucket, storage.MustNewObjectKey("second"), nil, bytes.NewReader(content), nil, nil)
	require.NoError(t, err)
	require.Len(t, physicalPartIDs(t, st), 2)
}

func TestCopyObjectSelfCopyKeepsSharedPartLive(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("object")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("self")), nil, nil)
	require.NoError(t, err)
	_, err = st.CopyObject(ctx, bucket, key, bucket, key, nil)
	require.NoError(t, err)
	assert.Equal(t, "self", readObjectContent(t, st, bucket, key, nil))
	require.Len(t, physicalPartIDs(t, st), 1)
}

func TestPostgresConcurrentDeleteAndCopyNeverDeletesLiveDestination(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	ctx := t.Context()
	st, cleanup := newPostgresTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	content := []byte("concurrent shared content")
	for i := range 100 {
		srcKey := storage.MustNewObjectKey(fmt.Sprintf("src-%d", i))
		dstKey := storage.MustNewObjectKey(fmt.Sprintf("dst-%d", i))
		_, err := st.PutObject(ctx, bucket, srcKey, nil, bytes.NewReader(content), nil, nil)
		require.NoError(t, err)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var copyErr, deleteErr error
		go func() {
			defer wg.Done()
			<-start
			_, copyErr = st.CopyObject(ctx, bucket, srcKey, bucket, dstKey, nil)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, deleteErr = st.DeleteObject(ctx, bucket, srcKey, nil)
		}()
		close(start)
		wg.Wait()
		require.NoError(t, deleteErr)
		if copyErr == nil {
			require.Equal(t, string(content), readObjectContent(t, st, bucket, dstKey, nil), "destination content at iteration %d", i)
			_, err = st.DeleteObject(ctx, bucket, dstKey, nil)
			require.NoError(t, err)
		} else {
			assert.ErrorIs(t, copyErr, storage.ErrNoSuchKey)
		}
	}
	assert.Empty(t, physicalPartIDs(t, st))
}

func TestPostgresConcurrentIdenticalUploadsRemainReadable(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	ctx := t.Context()
	st, cleanup := newPostgresTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	content := []byte("concurrent identical content")
	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = st.PutObject(ctx, bucket, storage.MustNewObjectKey(fmt.Sprintf("object-%d", i)), nil, bytes.NewReader(content), nil, nil)
		}()
	}
	close(start)
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	for i := range 2 {
		assert.Equal(t, string(content), readObjectContent(t, st, bucket, storage.MustNewObjectKey(fmt.Sprintf("object-%d", i)), nil))
	}
	var indexCount int
	require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		return tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM part_dedup_index").Scan(&indexCount)
	}))
	assert.Equal(t, 1, indexCount)
}

func TestUploadPartCopySharesWholeSourcePart(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	srcKey := storage.MustNewObjectKey("src")
	dstKey := storage.MustNewObjectKey("dst")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err := st.PutObject(ctx, bucket, srcKey, nil, bytes.NewReader([]byte("whole-part")), nil, nil)
	require.NoError(t, err)
	upload, err := st.CreateMultipartUpload(ctx, bucket, dstKey, nil, nil, nil)
	require.NoError(t, err)
	_, err = st.UploadPartCopy(ctx, bucket, srcKey, bucket, dstKey, upload.UploadId, 1, nil)
	require.NoError(t, err)
	require.Len(t, physicalPartIDs(t, st), 1)
	_, err = st.CompleteMultipartUpload(ctx, bucket, dstKey, upload.UploadId, nil, nil)
	require.NoError(t, err)
	_, err = st.DeleteObject(ctx, bucket, srcKey, nil)
	require.NoError(t, err)
	assert.Equal(t, "whole-part", readObjectContent(t, st, bucket, dstKey, nil))
}

func TestUploadPartCopyRangeCreatesNewPart(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	srcKey := storage.MustNewObjectKey("src")
	dstKey := storage.MustNewObjectKey("dst")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err := st.PutObject(ctx, bucket, srcKey, nil, bytes.NewReader([]byte("abcdef")), nil, nil)
	require.NoError(t, err)
	upload, err := st.CreateMultipartUpload(ctx, bucket, dstKey, nil, nil, nil)
	require.NoError(t, err)
	start, end := int64(1), int64(4)
	_, err = st.UploadPartCopy(ctx, bucket, srcKey, bucket, dstKey, upload.UploadId, 1, &storage.UploadPartCopyOptions{Range: &storage.ByteRange{Start: &start, End: &end}})
	require.NoError(t, err)
	require.Len(t, physicalPartIDs(t, st), 2)
}

func TestUploadPartCopyMultiPartSourceCreatesNewPart(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	srcKey := storage.MustNewObjectKey("src")
	dstKey := storage.MustNewObjectKey("dst")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err := st.AppendObject(ctx, bucket, srcKey, bytes.NewReader([]byte("first")), nil, nil)
	require.NoError(t, err)
	_, err = st.AppendObject(ctx, bucket, srcKey, bytes.NewReader([]byte("second")), nil, nil)
	require.NoError(t, err)
	require.Len(t, physicalPartIDs(t, st), 2)
	upload, err := st.CreateMultipartUpload(ctx, bucket, dstKey, nil, nil, nil)
	require.NoError(t, err)
	_, err = st.UploadPartCopy(ctx, bucket, srcKey, bucket, dstKey, upload.UploadId, 1, nil)
	require.NoError(t, err)
	require.Len(t, physicalPartIDs(t, st), 3)
}

func TestUploadPartReplacesExistingPartNumber(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("multipart")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	upload, err := st.CreateMultipartUpload(ctx, bucket, key, nil, nil, nil)
	require.NoError(t, err)

	_, err = st.UploadPart(ctx, bucket, key, upload.UploadId, 1, bytes.NewReader([]byte("old")), nil)
	require.NoError(t, err)
	_, err = st.UploadPart(ctx, bucket, key, upload.UploadId, 1, bytes.NewReader([]byte("new")), nil)
	require.NoError(t, err)

	var partIds []partstore.PartId
	require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: true}, func(_ context.Context, tx database.Tx) error {
		partIds, err = st.partStores.Default().GetPartIds(ctx, tx)
		return err
	}))
	require.Len(t, partIds, 1)
	var refCount int64
	require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: true}, func(_ context.Context, tx database.Tx) error {
		return tx.SqlTx().QueryRowContext(ctx, "SELECT ref_count FROM part_registry WHERE part_id = $1", partIds[0].String()).Scan(&refCount)
	}))
	assert.Equal(t, int64(1), refCount)

	_, err = st.CompleteMultipartUpload(ctx, bucket, key, upload.UploadId, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "new", readObjectContent(t, st, bucket, key, nil))
}

func TestObjectTaggingTargetsExplicitVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	enableVersioning(t, st, bucket)

	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("old")), nil, &storage.PutObjectOptions{Tags: map[string]string{"version": "old"}})
	require.NoError(t, err)
	_, err = st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("new")), nil, &storage.PutObjectOptions{Tags: map[string]string{"version": "new"}})
	require.NoError(t, err)
	latestVersion, olderVersions := versionIDsForKey(t, st, bucket, key)
	require.Len(t, olderVersions, 1)

	err = st.PutObjectTagging(ctx, bucket, key, map[string]string{"version": "retagged-old"}, &storage.ObjectTaggingOptions{VersionID: &olderVersions[0]})
	require.NoError(t, err)

	oldTags, err := st.GetObjectTagging(ctx, bucket, key, &storage.ObjectTaggingOptions{VersionID: &olderVersions[0]})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"version": "retagged-old"}, oldTags)
	latestTags, err := st.GetObjectTagging(ctx, bucket, key, &storage.ObjectTaggingOptions{VersionID: &latestVersion})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"version": "new"}, latestTags)

	err = st.DeleteObjectTagging(ctx, bucket, key, &storage.ObjectTaggingOptions{VersionID: &olderVersions[0]})
	require.NoError(t, err)
	oldTags, err = st.GetObjectTagging(ctx, bucket, key, &storage.ObjectTaggingOptions{VersionID: &olderVersions[0]})
	require.NoError(t, err)
	assert.Empty(t, oldTags)
	latestTags, err = st.GetObjectTagging(ctx, bucket, key, nil)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"version": "new"}, latestTags)
}

func TestObjectTaggingExplicitDeleteMarkerReturnsMethodNotAllowed(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	enableVersioning(t, st, bucket)

	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("data")), nil, nil)
	require.NoError(t, err)
	deleteResult, err := st.DeleteObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	require.NotNil(t, deleteResult.VersionID)

	_, err = st.GetObjectTagging(ctx, bucket, key, &storage.ObjectTaggingOptions{VersionID: deleteResult.VersionID})
	var methodNotAllowed *storage.VersionDeleteMarkerMethodNotAllowedError
	require.ErrorAs(t, err, &methodNotAllowed)
	assert.Equal(t, *deleteResult.VersionID, methodNotAllowed.VersionID)
}

func TestConditionalDeleteObject_MatchingETag(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	result, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	etag := *result.ETag

	// Delete with correct ETag — should succeed.
	_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{IfMatchETag: ptrutils.ToPtr(etag)})
	require.NoError(t, err)

	// Object should be gone.
	_, err = st.HeadObject(ctx, bucket, key, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
}

func TestConditionalDeleteObject_WrongETag(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	// Delete with wrong ETag — should return PreconditionFailed.
	_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{IfMatchETag: ptrutils.ToPtr("wrong-etag")})
	assert.ErrorIs(t, err, storage.ErrPreconditionFailed)

	// Object should still exist.
	_, err = st.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
}

func TestConditionalDeleteObject_NoObjectWithCondition(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("nonexistent")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Delete non-existent object with ETag condition — should return PreconditionFailed.
	_, err := st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{IfMatchETag: ptrutils.ToPtr("any-etag")})
	assert.ErrorIs(t, err, storage.ErrPreconditionFailed)
}

func TestConditionalDeleteObject_NoObjectNoCondition(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("nonexistent")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Delete non-existent object without condition — should silently succeed (S3 semantics).
	_, err := st.DeleteObject(ctx, bucket, key, nil)
	require.NoError(t, err)
}

func TestConditionalDeleteObjects_MixedConditions(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	key1 := storage.MustNewObjectKey("obj1")
	key2 := storage.MustNewObjectKey("obj2")
	key3 := storage.MustNewObjectKey("obj3")

	res1, err := st.PutObject(ctx, bucket, key1, nil, bytes.NewReader([]byte("data1")), nil, nil)
	require.NoError(t, err)
	etag1 := *res1.ETag

	_, err = st.PutObject(ctx, bucket, key2, nil, bytes.NewReader([]byte("data2")), nil, nil)
	require.NoError(t, err)

	_, err = st.PutObject(ctx, bucket, key3, nil, bytes.NewReader([]byte("data3")), nil, nil)
	require.NoError(t, err)

	entries := []storage.DeleteObjectsInputEntry{
		{Key: key1, IfMatchETag: ptrutils.ToPtr(etag1)},        // correct etag → deleted
		{Key: key2, IfMatchETag: ptrutils.ToPtr("wrong-etag")}, // wrong etag → error entry
		{Key: key3}, // no condition → deleted
	}
	deleteResult, err := st.DeleteObjects(ctx, bucket, entries)
	require.NoError(t, err)
	require.Len(t, deleteResult.Entries, 3)

	// Find entries by key.
	byKey := make(map[string]storage.DeleteObjectsEntry)
	for _, e := range deleteResult.Entries {
		byKey[e.Key.String()] = e
	}

	assert.True(t, byKey["obj1"].Deleted, "obj1 should be deleted (correct etag)")
	assert.False(t, byKey["obj2"].Deleted, "obj2 should NOT be deleted (wrong etag)")
	assert.Equal(t, "PreconditionFailed", byKey["obj2"].ErrCode)
	assert.True(t, byKey["obj3"].Deleted, "obj3 should be deleted (no condition)")

	// Verify obj1 and obj3 are gone, obj2 still exists.
	_, err = st.HeadObject(ctx, bucket, key1, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
	_, err = st.HeadObject(ctx, bucket, key2, nil)
	require.NoError(t, err)
	_, err = st.HeadObject(ctx, bucket, key3, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
}

func TestDeleteObjects_KeyOnlyDeleteReturnsDeleteMarkerVersionID(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	status := storage.BucketVersioningStatusEnabled
	require.NoError(t, st.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &status}))

	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	deleteResult, err := st.DeleteObjects(ctx, bucket, []storage.DeleteObjectsInputEntry{{Key: key}})
	require.NoError(t, err)
	require.Len(t, deleteResult.Entries, 1)

	entry := deleteResult.Entries[0]
	assert.True(t, entry.Deleted)
	require.NotNil(t, entry.DeleteMarker)
	assert.True(t, *entry.DeleteMarker)
	assert.Nil(t, entry.VersionID)
	require.NotNil(t, entry.DeleteMarkerVersionID)
	assert.NotEmpty(t, *entry.DeleteMarkerVersionID)

	_, err = st.HeadObject(ctx, bucket, key, nil)
	var currentDeleteMarkerErr *storage.CurrentDeleteMarkerError
	assert.ErrorAs(t, err, &currentDeleteMarkerErr)
	require.NotNil(t, currentDeleteMarkerErr)
	assert.Equal(t, *entry.DeleteMarkerVersionID, currentDeleteMarkerErr.VersionID)
}

func TestDeleteObjectSuspendedRemovesNullVersionParts(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("data")), nil, nil)
	require.NoError(t, err)
	status := storage.BucketVersioningStatusSuspended
	require.NoError(t, st.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &status}))

	_, err = st.DeleteObject(ctx, bucket, key, nil)
	require.NoError(t, err)

	var partRowCount int
	require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: true}, func(_ context.Context, tx database.Tx) error {
		return tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM parts").Scan(&partRowCount)
	}))
	assert.Zero(t, partRowCount)

	var partIds []partstore.PartId
	require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: true}, func(_ context.Context, tx database.Tx) error {
		partIds, err = st.partStores.Default().GetPartIds(ctx, tx)
		return err
	}))
	assert.Empty(t, partIds)
}

func TestListObjectVersions_CommonPrefixWithMultipleKeysNotRepeatedAcrossPages(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	status := storage.BucketVersioningStatusEnabled
	require.NoError(t, st.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &status}))

	for _, key := range []string{"a/one", "a/two", "b/one"} {
		_, err := st.PutObject(ctx, bucket, storage.MustNewObjectKey(key), nil, bytes.NewReader([]byte("data")), nil, nil)
		require.NoError(t, err)
	}

	delimiter := "/"
	page1, err := st.ListObjectVersions(ctx, bucket, storage.ListObjectVersionsOptions{Delimiter: &delimiter, MaxKeys: 1})
	require.NoError(t, err)
	require.Equal(t, []string{"a/"}, page1.CommonPrefixes)
	require.True(t, page1.IsTruncated)
	require.NotNil(t, page1.NextKeyMarker)

	page2, err := st.ListObjectVersions(ctx, bucket, storage.ListObjectVersionsOptions{Delimiter: &delimiter, MaxKeys: 1, KeyMarker: page1.NextKeyMarker, VersionIDMarker: page1.NextVersionIDMarker})
	require.NoError(t, err)
	assert.Equal(t, []string{"b/"}, page2.CommonPrefixes)
	assert.False(t, page2.IsTruncated)
}

func TestListObjectVersions_PaginationIncludesNullVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Unversioned put creates the "null" version, then newer ULID versions
	// are stacked on top after enabling versioning.
	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("v-null")), nil, nil)
	require.NoError(t, err)

	status := storage.BucketVersioningStatusEnabled
	require.NoError(t, st.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &status}))

	_, err = st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("v2")), nil, nil)
	require.NoError(t, err)
	_, err = st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("v3")), nil, nil)
	require.NoError(t, err)

	collected := map[string]int{}
	opts := storage.ListObjectVersionsOptions{MaxKeys: 1}
	for range 10 {
		page, err := st.ListObjectVersions(ctx, bucket, opts)
		require.NoError(t, err)
		for _, version := range page.Versions {
			collected[version.VersionID]++
		}
		if !page.IsTruncated {
			break
		}
		opts.KeyMarker = page.NextKeyMarker
		opts.VersionIDMarker = page.NextVersionIDMarker
	}

	assert.Len(t, collected, 3)
	assert.Equal(t, 1, collected["null"], "null version must appear exactly once across pages")
	for versionID, count := range collected {
		assert.Equal(t, 1, count, "version %s listed %d times", versionID, count)
	}
}

func TestConditionalDeleteObject_WildcardMatchExistingObject(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	// Delete with If-Match: * on an existing object — should succeed.
	_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{IfMatchETag: ptrutils.ToPtr(storage.ETagWildcard)})
	require.NoError(t, err)

	// Object should be gone.
	_, err = st.HeadObject(ctx, bucket, key, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
}

func TestConditionalDeleteObject_WildcardMatchMissingObject(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("nonexistent")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Delete with If-Match: * on a non-existent object — should return PreconditionFailed.
	_, err := st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{IfMatchETag: ptrutils.ToPtr(storage.ETagWildcard)})
	assert.ErrorIs(t, err, storage.ErrPreconditionFailed)
}

// --- AppendObject tests ---

func TestAppendObject_CreateOnMissing(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Append to a non-existent key — should behave like PutObject.
	result, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.NotEmpty(t, result.ETag)
	assert.Equal(t, int64(5), result.Size)

	obj, err := st.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(5), obj.Size)
}

func TestAppendObject_AppendsToExisting(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	var before *metadatastore.Object
	require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: true}, func(_ context.Context, tx database.Tx) error {
		before, err = st.metadataStore.HeadObject(ctx, tx.SqlTx(), bucket, key)
		return err
	}))
	require.Len(t, before.Parts, 1)
	firstPartID := before.Parts[0].Id

	result, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(11), result.Size)
	var after *metadatastore.Object
	require.NoError(t, database.WithTx(ctx, st.db, &sql.TxOptions{ReadOnly: true}, func(_ context.Context, tx database.Tx) error {
		after, err = st.metadataStore.HeadObject(ctx, tx.SqlTx(), bucket, key)
		return err
	}))
	require.Len(t, after.Parts, 2)
	assert.Equal(t, firstPartID, after.Parts[0].Id)

	// Read back and verify the concatenated content.
	_, readers, err := st.GetObject(ctx, bucket, key, nil, nil)
	require.NoError(t, err)
	require.Len(t, readers, 1)
	defer readers[0].Close()
	content, err := io.ReadAll(readers[0])
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(content))
}

func TestAppendObject_ETagIsMultipartStyle(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("part1")), nil, nil)
	require.NoError(t, err)

	result, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("part2")), nil, nil)
	require.NoError(t, err)

	// After two appends the object has 2 parts, so ETag must end in "-2".
	// ETags are stored with surrounding double-quotes, e.g. `"abc123-2"`.
	assert.True(t, len(result.ETag) > 3 && result.ETag[len(result.ETag)-3:] == "-2\"",
		"expected multipart ETag ending in -2\", got %q", result.ETag)
}

func TestAppendObject_CorrectWriteOffset(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	first, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	// Append with the correct write offset (== current size) — should succeed.
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil,
		&storage.AppendObjectOptions{WriteOffset: &first.Size})
	require.NoError(t, err)
}

func TestAppendObject_WrongWriteOffset(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	// Append with a wrong write offset — should return ErrInvalidWriteOffset.
	wrongOffset := int64(999)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil,
		&storage.AppendObjectOptions{WriteOffset: &wrongOffset})
	assert.ErrorIs(t, err, storage.ErrInvalidWriteOffset)
}

func TestAppendObject_WriteOffsetZeroOnMissing(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// WriteOffset == 0 on a non-existent object should succeed (create new object).
	zero := int64(0)
	result, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil,
		&storage.AppendObjectOptions{WriteOffset: &zero})
	require.NoError(t, err)
	assert.Equal(t, int64(5), result.Size)
}

func TestAppendObject_WriteOffsetNonZeroOnMissing(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// WriteOffset != 0 on a non-existent object should return ErrInvalidWriteOffset.
	nonZero := int64(5)
	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil,
		&storage.AppendObjectOptions{WriteOffset: &nonZero})
	assert.ErrorIs(t, err, storage.ErrInvalidWriteOffset)
}

func TestAppendObject_CreatesNewVersionWhenVersioningEnabled(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	status := storage.BucketVersioningStatusEnabled
	require.NoError(t, st.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &status}))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	require.Len(t, physicalPartIDs(t, st), 1)

	result, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(11), result.Size)
	require.Len(t, physicalPartIDs(t, st), 2)

	latestVersionID, olderVersionIDs := versionIDsForKey(t, st, bucket, key)
	require.Len(t, olderVersionIDs, 1)
	assert.NotEqual(t, "null", latestVersionID)
	assert.NotEqual(t, "null", olderVersionIDs[0])
	assert.NotEqual(t, latestVersionID, olderVersionIDs[0])

	assert.Equal(t, "hello world", readObjectContent(t, st, bucket, key, nil))
	assert.Equal(t, "hello world", readObjectContent(t, st, bucket, key, &latestVersionID))
	assert.Equal(t, "hello", readObjectContent(t, st, bucket, key, &olderVersionIDs[0]))
}

func TestAppendObject_CreatesNewVersionAfterDeleteMarkerWhenVersioningEnabled(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	status := storage.BucketVersioningStatusEnabled
	require.NoError(t, st.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &status}))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	_, err = st.DeleteObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("world")), nil, nil)
	require.NoError(t, err)

	latestVersionID, olderVersionIDs := versionIDsForKey(t, st, bucket, key)
	require.Len(t, olderVersionIDs, 1)
	assert.NotEqual(t, latestVersionID, olderVersionIDs[0])
	assert.Equal(t, "world", readObjectContent(t, st, bucket, key, nil))
	assert.Equal(t, "world", readObjectContent(t, st, bucket, key, &latestVersionID))
	assert.Equal(t, "hello", readObjectContent(t, st, bucket, key, &olderVersionIDs[0]))
}

func TestAppendObject_DeleteObjectVersionPreservesOlderVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	status := storage.BucketVersioningStatusEnabled
	require.NoError(t, st.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &status}))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil, nil)
	require.NoError(t, err)

	latestVersionID, olderVersionIDs := versionIDsForKey(t, st, bucket, key)
	require.Len(t, olderVersionIDs, 1)

	_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: &latestVersionID})
	require.NoError(t, err)

	assert.Equal(t, "hello", readObjectContent(t, st, bucket, key, nil))
	assert.Equal(t, "hello", readObjectContent(t, st, bucket, key, &olderVersionIDs[0]))

	_, err = st.HeadObject(ctx, bucket, key, &storage.HeadObjectOptions{VersionID: &latestVersionID})
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
}

func TestAppendObject_DeleteObjectsVersionPreservesOlderVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	status := storage.BucketVersioningStatusEnabled
	require.NoError(t, st.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &status}))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil, nil)
	require.NoError(t, err)

	latestVersionID, olderVersionIDs := versionIDsForKey(t, st, bucket, key)
	require.Len(t, olderVersionIDs, 1)

	deleteResult, err := st.DeleteObjects(ctx, bucket, []storage.DeleteObjectsInputEntry{{Key: key, VersionID: &latestVersionID}})
	require.NoError(t, err)
	require.Len(t, deleteResult.Entries, 1)
	assert.True(t, deleteResult.Entries[0].Deleted)

	assert.Equal(t, "hello", readObjectContent(t, st, bucket, key, nil))
	assert.Equal(t, "hello", readObjectContent(t, st, bucket, key, &olderVersionIDs[0]))

	_, err = st.HeadObject(ctx, bucket, key, &storage.HeadObjectOptions{VersionID: &latestVersionID})
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
}

func TestAppendObject_SuspendedBucketReusesNullVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	status := storage.BucketVersioningStatusSuspended
	require.NoError(t, st.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &status}))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	result, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(11), result.Size)

	latestVersionID, olderVersionIDs := versionIDsForKey(t, st, bucket, key)
	assert.Equal(t, "null", latestVersionID)
	assert.Empty(t, olderVersionIDs)
	assert.Equal(t, "hello world", readObjectContent(t, st, bucket, key, nil))
}

func TestAppendObject_NoSuchBucket(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("nonexistent")
	key := storage.MustNewObjectKey("obj")

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("data")), nil, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchBucket)
}

func TestAppendObject_TooManyParts(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("stress")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Seed an object with exactly 10,000 parts directly via the metadata store
	// so we don't have to perform 10,000 real appends (which would be O(n²) in
	// SQLite and cause CI timeouts).
	const maxParts = 10_000
	parts := make([]metadatastore.Part, maxParts)
	partChecksumsList := make([]checksumutils.PartChecksums, maxParts)

	// ETag of a single byte "x", as produced by CalculateChecksumsStreaming.
	partData := []byte("x")
	_, partChecksums, err := checksumutils.CalculateChecksumsStreaming(ctx, bytes.NewReader(partData), func(r io.Reader) error { return nil })
	require.NoError(t, err)
	partETag := *partChecksums.ETag

	for i := range parts {
		pid, err := partstore.NewRandomPartId()
		require.NoError(t, err)
		parts[i] = metadatastore.Part{
			Id:   *pid,
			ETag: partETag,
			Size: int64(len(partData)),
		}
		partChecksumsList[i] = checksumutils.PartChecksums{ETag: partETag, Size: int64(len(partData))}
	}

	objectChecksums, err := checksumutils.CalculateMultipartChecksums(partChecksumsList, checksumutils.ChecksumTypeFullObject)
	require.NoError(t, err)

	err = database.WithTx(ctx, st.db, nil, func(ctx context.Context, tx database.Tx) error {
		for _, p := range parts {
			if err := st.partStores.Default().PutPart(ctx, tx, p.Id, bytes.NewReader(partData)); err != nil {
				return err
			}
		}
		_, err := st.metadataStore.PutObject(ctx, tx.SqlTx(), bucket, &metadatastore.Object{
			Key:   key,
			ETag:  *objectChecksums.ETag,
			Size:  int64(maxParts) * int64(len(partData)),
			Parts: parts,
		}, nil)
		return err
	})
	require.NoError(t, err)

	// The next append must fail with ErrTooManyParts.
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("x")), nil, nil)
	assert.ErrorIs(t, err, storage.ErrTooManyParts)
}

func TestGetObject_RangeRegression_StartInsideLaterPart(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("part-0000-")), nil, nil)
	require.NoError(t, err)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("part-1111-")), nil, nil)
	require.NoError(t, err)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("part-2222")), nil, nil)
	require.NoError(t, err)

	start := int64(12) // inside second part
	end := int64(26)   // inside third part
	_, readers, err := st.GetObject(ctx, bucket, key, []storage.ByteRange{{Start: &start, End: &end}}, nil)
	require.NoError(t, err)
	require.Len(t, readers, 1)
	defer readers[0].Close()

	content, err := io.ReadAll(readers[0])
	require.NoError(t, err)
	assert.Equal(t, "rt-1111-part-2", string(content))
}

func TestGetObject_RangeHandlingAcrossPartBoundaries(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil, nil)
	require.NoError(t, err)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("!!!")), nil, nil)
	require.NoError(t, err)

	tests := []struct {
		name      string
		byteRange storage.ByteRange
		expected  string
	}{
		{
			name:      "entire second part",
			byteRange: storage.ByteRange{Start: ptrutils.ToPtr(int64(5)), End: ptrutils.ToPtr(int64(11))},
			expected:  " world",
		},
		{
			name:      "straddles second and third part",
			byteRange: storage.ByteRange{Start: ptrutils.ToPtr(int64(8)), End: ptrutils.ToPtr(int64(13))},
			expected:  "rld!!",
		},
		{
			name:      "open-ended from middle of second part",
			byteRange: storage.ByteRange{Start: ptrutils.ToPtr(int64(7)), End: nil},
			expected:  "orld!!!",
		},
		{
			name:      "exact part boundary",
			byteRange: storage.ByteRange{Start: ptrutils.ToPtr(int64(11)), End: ptrutils.ToPtr(int64(14))},
			expected:  "!!!",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, readers, err := st.GetObject(ctx, bucket, key, []storage.ByteRange{tc.byteRange}, nil)
			require.NoError(t, err)
			require.Len(t, readers, 1)
			defer readers[0].Close()

			content, err := io.ReadAll(readers[0])
			require.NoError(t, err)
			assert.Equal(t, tc.expected, string(content))
		})
	}
}

func TestGetObject_MultipleRangesStartingAfterEarlierParts(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("aaa")), nil, nil)
	require.NoError(t, err)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("bbb")), nil, nil)
	require.NoError(t, err)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("ccc")), nil, nil)
	require.NoError(t, err)

	ranges := []storage.ByteRange{
		{Start: ptrutils.ToPtr(int64(4)), End: ptrutils.ToPtr(int64(6))},
		{Start: ptrutils.ToPtr(int64(7)), End: ptrutils.ToPtr(int64(9))},
	}

	_, readers, err := st.GetObject(ctx, bucket, key, ranges, nil)
	require.NoError(t, err)
	require.Len(t, readers, 2)
	defer readers[0].Close()
	defer readers[1].Close()

	first, err := io.ReadAll(readers[0])
	require.NoError(t, err)
	second, err := io.ReadAll(readers[1])
	require.NoError(t, err)

	assert.Equal(t, "bb", string(first))
	assert.Equal(t, "cc", string(second))
}
