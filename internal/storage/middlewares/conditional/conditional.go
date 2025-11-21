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

func (csm *conditionalStorageMiddleware) lookupStorage(bucketName storage.BucketName) storage.Storage {
	storage, ok := csm.bucketToStorageMap[bucketName.String()]
	if ok {
		return storage
	}
	return csm.defaultStorage
}

func (csm *conditionalStorageMiddleware) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	storage := csm.lookupStorage(bucketName)
	return storage.CreateBucket(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	storage := csm.lookupStorage(bucketName)
	return storage.DeleteBucket(ctx, bucketName)
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
	slices.SortFunc(allBuckets, func(a storage.Bucket, b storage.Bucket) int { return strings.Compare(a.Name.String(), b.Name.String()) })
	return allBuckets, nil
}

func (csm *conditionalStorageMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	storage := csm.lookupStorage(bucketName)
	return storage.HeadBucket(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, prefix string, delimiter string, startAfter string, maxKeys int32) (*storage.ListBucketResult, error) {
	storage := csm.lookupStorage(bucketName)
	return storage.ListObjects(ctx, bucketName, prefix, delimiter, startAfter, maxKeys)
}

func (csm *conditionalStorageMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (*storage.Object, error) {
	storage := csm.lookupStorage(bucketName)
	return storage.HeadObject(ctx, bucketName, key)
}

func (csm *conditionalStorageMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	storage := csm.lookupStorage(bucketName)
	return storage.GetObject(ctx, bucketName, key, startByte, endByte)
}

func (csm *conditionalStorageMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	storage := csm.lookupStorage(bucketName)
	return storage.PutObject(ctx, bucketName, key, contentType, reader, checksumInput)
}

func (csm *conditionalStorageMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	storage := csm.lookupStorage(bucketName)
	return storage.DeleteObject(ctx, bucketName, key)
}

func (csm *conditionalStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	storage := csm.lookupStorage(bucketName)
	return storage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
}

func (csm *conditionalStorageMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId string, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	storage := csm.lookupStorage(bucketName)
	return storage.UploadPart(ctx, bucketName, key, uploadId, partNumber, reader, checksumInput)
}

func (csm *conditionalStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId string, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	storage := csm.lookupStorage(bucketName)
	return storage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput)
}

func (csm *conditionalStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId string) error {
	storage := csm.lookupStorage(bucketName)
	return storage.AbortMultipartUpload(ctx, bucketName, key, uploadId)
}

func (csm *conditionalStorageMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*storage.ListMultipartUploadsResult, error) {
	storage := csm.lookupStorage(bucketName)
	return storage.ListMultipartUploads(ctx, bucketName, prefix, delimiter, keyMarker, uploadIdMarker, maxUploads)
}

func (csm *conditionalStorageMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId string, partNumberMarker string, maxParts int32) (*storage.ListPartsResult, error) {
	storage := csm.lookupStorage(bucketName)
	return storage.ListParts(ctx, bucketName, key, uploadId, partNumberMarker, maxParts)
}
