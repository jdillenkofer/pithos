package conditional

import (
	"context"
	"io"
	"slices"
	"strings"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type conditionalStorageMiddleware struct {
	*lifecycle.ValidatedLifecycle
	bucketToStorageMap map[string]storage.Storage
	defaultStorage     storage.Storage
}

// Compile-time check to ensure conditionalStorageMiddleware implements storage.Storage
var _ storage.Storage = (*conditionalStorageMiddleware)(nil)

func NewStorageMiddleware(bucketToStorageMap map[string]storage.Storage, defaultStorage storage.Storage) (storage.Storage, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("ConditionalStorageMiddleware")
	if err != nil {
		return nil, err
	}

	return &conditionalStorageMiddleware{
		ValidatedLifecycle: lifecycle,
		bucketToStorageMap: bucketToStorageMap,
		defaultStorage:     defaultStorage,
	}, nil
}

func (csm *conditionalStorageMiddleware) Start(ctx context.Context) error {
	if err := csm.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}

	for _, bucketStorage := range csm.bucketToStorageMap {
		if err := bucketStorage.Start(ctx); err != nil {
			return err
		}
	}

	return csm.defaultStorage.Start(ctx)
}

func (csm *conditionalStorageMiddleware) Stop(ctx context.Context) error {
	if err := csm.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}

	for _, bucketStorage := range csm.bucketToStorageMap {
		if err := bucketStorage.Stop(ctx); err != nil {
			return err
		}
	}

	return csm.defaultStorage.Stop(ctx)
}

func (csm *conditionalStorageMiddleware) lookupStorage(bucket string) storage.Storage {
	storage, ok := csm.bucketToStorageMap[bucket]
	if ok {
		return storage
	}
	return csm.defaultStorage
}

func (csm *conditionalStorageMiddleware) CreateBucket(ctx context.Context, bucket string) error {
	storage := csm.lookupStorage(bucket)
	return storage.CreateBucket(ctx, bucket)
}

func (csm *conditionalStorageMiddleware) DeleteBucket(ctx context.Context, bucket string) error {
	storage := csm.lookupStorage(bucket)
	return storage.DeleteBucket(ctx, bucket)
}

func (csm *conditionalStorageMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	allBuckets := []storage.Bucket{}
	for _, bucketStorage := range csm.bucketToStorageMap {
		buckets, err := bucketStorage.ListBuckets(ctx)
		if err != nil {
			return nil, err
		}
		allBuckets = append(allBuckets, buckets...)
	}
	slices.SortFunc(allBuckets, func(a storage.Bucket, b storage.Bucket) int { return strings.Compare(a.Name, b.Name) })
	return allBuckets, nil
}

func (csm *conditionalStorageMiddleware) HeadBucket(ctx context.Context, bucket string) (*storage.Bucket, error) {
	storage := csm.lookupStorage(bucket)
	return storage.HeadBucket(ctx, bucket)
}

func (csm *conditionalStorageMiddleware) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int32) (*storage.ListBucketResult, error) {
	storage := csm.lookupStorage(bucket)
	return storage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
}

func (csm *conditionalStorageMiddleware) HeadObject(ctx context.Context, bucket string, key string) (*storage.Object, error) {
	storage := csm.lookupStorage(bucket)
	return storage.HeadObject(ctx, bucket, key)
}

func (csm *conditionalStorageMiddleware) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	storage := csm.lookupStorage(bucket)
	return storage.GetObject(ctx, bucket, key, startByte, endByte)
}

func (csm *conditionalStorageMiddleware) PutObject(ctx context.Context, bucket string, key string, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	storage := csm.lookupStorage(bucket)
	return storage.PutObject(ctx, bucket, key, contentType, reader, checksumInput)
}

func (csm *conditionalStorageMiddleware) DeleteObject(ctx context.Context, bucket string, key string) error {
	storage := csm.lookupStorage(bucket)
	return storage.DeleteObject(ctx, bucket, key)
}

func (csm *conditionalStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucket string, key string, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	storage := csm.lookupStorage(bucket)
	return storage.CreateMultipartUpload(ctx, bucket, key, contentType, checksumType)
}

func (csm *conditionalStorageMiddleware) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	storage := csm.lookupStorage(bucket)
	return storage.UploadPart(ctx, bucket, key, uploadId, partNumber, reader, checksumInput)
}

func (csm *conditionalStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	storage := csm.lookupStorage(bucket)
	return storage.CompleteMultipartUpload(ctx, bucket, key, uploadId, checksumInput)
}

func (csm *conditionalStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	storage := csm.lookupStorage(bucket)
	return storage.AbortMultipartUpload(ctx, bucket, key, uploadId)
}

func (csm *conditionalStorageMiddleware) ListMultipartUploads(ctx context.Context, bucket string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*storage.ListMultipartUploadsResult, error) {
	storage := csm.lookupStorage(bucket)
	return storage.ListMultipartUploads(ctx, bucket, prefix, delimiter, keyMarker, uploadIdMarker, maxUploads)
}

func (csm *conditionalStorageMiddleware) ListParts(ctx context.Context, bucket string, key string, uploadId string, partNumberMarker string, maxParts int32) (*storage.ListPartsResult, error) {
	storage := csm.lookupStorage(bucket)
	return storage.ListParts(ctx, bucket, key, uploadId, partNumberMarker, maxParts)
}
