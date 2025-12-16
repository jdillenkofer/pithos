package conditional

import (
	"context"
	"io"
	"slices"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type conditionalStorageMiddleware struct {
	*lifecycle.ValidatedLifecycle
	bucketToStorageMap map[string]storage.Storage
	defaultStorage     storage.Storage
	tracer             trace.Tracer
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
		tracer:             otel.Tracer("internal/storage/middlewares/conditional"),
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
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.CreateBucket")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.CreateBucket(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.DeleteBucket")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.DeleteBucket(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.ListBuckets")
	defer span.End()

	allBuckets := []storage.Bucket{}

	// Include buckets from all specific storages
	for _, bucketStorage := range csm.bucketToStorageMap {
		buckets, err := bucketStorage.ListBuckets(ctx)
		if err != nil {
			return nil, err
		}
		allBuckets = append(allBuckets, buckets...)
	}

	// Include buckets from default storage
	buckets, err := csm.defaultStorage.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	allBuckets = append(allBuckets, buckets...)

	slices.SortFunc(allBuckets, func(a storage.Bucket, b storage.Bucket) int { return strings.Compare(a.Name.String(), b.Name.String()) })
	return allBuckets, nil
}

func (csm *conditionalStorageMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.HeadBucket")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.HeadBucket(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.ListObjects")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.ListObjects(ctx, bucketName, opts)
}

func (csm *conditionalStorageMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (*storage.Object, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.HeadObject")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.HeadObject(ctx, bucketName, key)
}

func (csm *conditionalStorageMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.GetObject")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.GetObject(ctx, bucketName, key, ranges)
}

func (csm *conditionalStorageMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.PutObject")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.PutObject(ctx, bucketName, key, contentType, reader, checksumInput)
}

func (csm *conditionalStorageMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.DeleteObject")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.DeleteObject(ctx, bucketName, key)
}

func (csm *conditionalStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.CreateMultipartUpload")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
}

func (csm *conditionalStorageMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.UploadPart")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.UploadPart(ctx, bucketName, key, uploadId, partNumber, reader, checksumInput)
}

func (csm *conditionalStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.CompleteMultipartUpload")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput)
}

func (csm *conditionalStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.AbortMultipartUpload")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.AbortMultipartUpload(ctx, bucketName, key, uploadId)
}

func (csm *conditionalStorageMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.ListMultipartUploads")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.ListMultipartUploads(ctx, bucketName, opts)
}

func (csm *conditionalStorageMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.ListParts")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.ListParts(ctx, bucketName, key, uploadId, opts)
}
