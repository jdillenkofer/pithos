package conditional

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	sqlPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

type copyRecordingStorage struct {
	storage.Storage
	copyObjectCalls     int
	uploadPartCopyCalls int
}

func (s *copyRecordingStorage) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	s.copyObjectCalls++
	return &storage.CopyObjectResult{}, nil
}

func (s *copyRecordingStorage) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	s.uploadPartCopyCalls++
	return &storage.UploadPartCopyResult{}, nil
}

type copySourceStorage struct {
	storage.Storage
	body []byte
}

func (s *copySourceStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	return &storage.Object{
		Key:          key,
		LastModified: time.Now(),
		ETag:         "\"source-etag\"",
		Size:         int64(len(s.body)),
	}, nil
}

func (s *copySourceStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	obj, err := s.HeadObject(ctx, bucketName, key, nil)
	if err != nil {
		return nil, nil, err
	}
	return obj, []io.ReadCloser{io.NopCloser(bytes.NewReader(s.body))}, nil
}

type seekableDestinationStorage struct {
	storage.Storage
	putObjectSawSeekable  bool
	uploadPartSawSeekable bool
	receivedBody          []byte
}

func (s *seekableDestinationStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	_, s.putObjectSawSeekable = data.(io.Seeker)
	body, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	s.receivedBody = body
	etag := "\"dest-etag\""
	return &storage.PutObjectResult{ETag: &etag}, nil
}

func (s *seekableDestinationStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	_, s.uploadPartSawSeekable = data.(io.Seeker)
	body, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	s.receivedBody = body
	return &storage.UploadPartResult{ETag: "\"part-etag\""}, nil
}

func TestConditionalStorageRoutesCopyOperationsByDestinationBucket(t *testing.T) {
	testutils.SkipIfIntegration(t)

	defaultStorage := &copyRecordingStorage{}
	mappedStorage := &copyRecordingStorage{}
	conditionalStorage, err := NewStorageMiddleware(map[string]storage.Storage{
		"bucket": mappedStorage,
	}, defaultStorage)
	assert.Nil(t, err)

	ctx := context.Background()
	bucket := storage.MustNewBucketName("bucket")
	srcKey := storage.MustNewObjectKey("src")
	dstKey := storage.MustNewObjectKey("dst")
	uploadId := storage.MustNewUploadId("upload-id")

	_, err = conditionalStorage.CopyObject(ctx, bucket, srcKey, bucket, dstKey, nil)
	assert.Nil(t, err)
	_, err = conditionalStorage.UploadPartCopy(ctx, bucket, srcKey, bucket, dstKey, uploadId, 1, nil)
	assert.Nil(t, err)

	assert.Equal(t, 1, mappedStorage.copyObjectCalls)
	assert.Equal(t, 1, mappedStorage.uploadPartCopyCalls)
	assert.Equal(t, 0, defaultStorage.copyObjectCalls)
	assert.Equal(t, 0, defaultStorage.uploadPartCopyCalls)
}

func TestConditionalStorageCrossBackendCopyObjectUsesSeekableDestinationBody(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := []byte("cross-backend-copy")
	srcStorage := &copySourceStorage{body: body}
	dstStorage := &seekableDestinationStorage{}
	conditionalStorage, err := NewStorageMiddleware(map[string]storage.Storage{
		"src-bucket": srcStorage,
	}, dstStorage)
	assert.Nil(t, err)

	_, err = conditionalStorage.CopyObject(context.Background(), storage.MustNewBucketName("src-bucket"), storage.MustNewObjectKey("src"), storage.MustNewBucketName("dst-bucket"), storage.MustNewObjectKey("dst"), nil)

	assert.Nil(t, err)
	assert.True(t, dstStorage.putObjectSawSeekable)
	assert.Equal(t, body, dstStorage.receivedBody)
}

func TestConditionalStorageCrossBackendUploadPartCopyUsesSeekableDestinationBody(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := []byte("cross-backend-upload-part-copy")
	srcStorage := &copySourceStorage{body: body}
	dstStorage := &seekableDestinationStorage{}
	conditionalStorage, err := NewStorageMiddleware(map[string]storage.Storage{
		"src-bucket": srcStorage,
	}, dstStorage)
	assert.Nil(t, err)

	_, err = conditionalStorage.UploadPartCopy(context.Background(), storage.MustNewBucketName("src-bucket"), storage.MustNewObjectKey("src"), storage.MustNewBucketName("dst-bucket"), storage.MustNewObjectKey("dst"), storage.MustNewUploadId("upload-id"), 1, nil)

	assert.Nil(t, err)
	assert.True(t, dstStorage.uploadPartSawSeekable)
	assert.Equal(t, body, dstStorage.receivedBody)
}

func TestConditionalStorage(t *testing.T) {
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
	dbPath2 := filepath.Join(storagePath, "pithos2.db")
	db2, err := sqlite.OpenDatabase(dbPath2)
	if err != nil {
		slog.Error("Couldn't open database 2")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = db2.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database 2 %s", err))
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

	metadataPartStorage, err := metadatapart.NewStorage(db, metadataStore, partStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create MetadataPartStorage: %s", err))
		os.Exit(1)
	}

	fsPartStore, err := filesystemPartStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemPartStore: %s", err))
		os.Exit(1)
	}

	bucketRepository2, err := repositoryFactory.NewBucketRepository(db2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BucketRepository 2: %s", err))
		os.Exit(1)
	}
	objectRepository2, err := repositoryFactory.NewObjectRepository(db2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ObjectRepository 2: %s", err))
		os.Exit(1)
	}
	tagRepository2, err := repositoryFactory.NewTagRepository(db2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create TagRepository: %s", err))
		os.Exit(1)
	}
	partRepository2, err := repositoryFactory.NewPartRepository(db2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartRepository 2: %s", err))
		os.Exit(1)
	}
	userMetadataRepository2, err := repositoryFactory.NewUserMetadataRepository(db2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create UserMetadataRepository 2: %s", err))
		os.Exit(1)
	}
	metadataStore2, err := sqlMetadataStore.New(db2, bucketRepository2, objectRepository2, partRepository2, tagRepository2, userMetadataRepository2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlMetadataStore 2: %s", err))
		os.Exit(1)
	}

	metadataPartStorage2, err := metadatapart.NewStorage(db2, metadataStore2, fsPartStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create MetadataPartStorage: %s", err))
		os.Exit(1)
	}

	bucketToStorageMap := map[string]storage.Storage{"bucket": metadataPartStorage}
	conditionalStorage, err := NewStorageMiddleware(bucketToStorageMap, metadataPartStorage2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ConditionalStorage: %s", err))
		os.Exit(1)
	}

	content := []byte("ConditionalStorage")
	err = storage.Tester(conditionalStorage, []storage.BucketName{storage.MustNewBucketName("bucket"), storage.MustNewBucketName("otherbucket")}, content)
	assert.Nil(t, err)
}
