package tracing

import (
	"context"
	"io"
	"runtime/trace"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type tracingStorageMiddleware struct {
	*lifecycle.ValidatedLifecycle
	regionName   string
	innerStorage storage.Storage
}

// Compile-time check to ensure tracingStorageMiddleware implements storage.Storage
var _ storage.Storage = (*tracingStorageMiddleware)(nil)

func NewStorageMiddleware(regionName string, innerStorage storage.Storage) (storage.Storage, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("TracingStorageMiddleware")
	if err != nil {
		return nil, err
	}

	return &tracingStorageMiddleware{
		ValidatedLifecycle: lifecycle,
		regionName:         regionName,
		innerStorage:       innerStorage,
	}, nil
}

func (tsm *tracingStorageMiddleware) Start(ctx context.Context) error {
	defer trace.StartRegion(ctx, tsm.regionName+".Start()").End()

	if err := tsm.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}

	return tsm.innerStorage.Start(ctx)
}

func (tsm *tracingStorageMiddleware) Stop(ctx context.Context) error {
	defer trace.StartRegion(ctx, tsm.regionName+".Stop()").End()

	if err := tsm.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}

	return tsm.innerStorage.Stop(ctx)
}

func (tsm *tracingStorageMiddleware) CreateBucket(ctx context.Context, bucket string) error {
	defer trace.StartRegion(ctx, tsm.regionName+".CreateBucket()").End()

	return tsm.innerStorage.CreateBucket(ctx, bucket)
}

func (tsm *tracingStorageMiddleware) DeleteBucket(ctx context.Context, bucket string) error {
	defer trace.StartRegion(ctx, tsm.regionName+".DeleteBucket()").End()

	return tsm.innerStorage.DeleteBucket(ctx, bucket)
}

func (tsm *tracingStorageMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListBuckets()").End()

	return tsm.innerStorage.ListBuckets(ctx)
}

func (tsm *tracingStorageMiddleware) HeadBucket(ctx context.Context, bucket string) (*storage.Bucket, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".HeadBucket()").End()

	return tsm.innerStorage.HeadBucket(ctx, bucket)
}

func (tsm *tracingStorageMiddleware) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int32) (*storage.ListBucketResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListObjects()").End()

	return tsm.innerStorage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
}

func (tsm *tracingStorageMiddleware) HeadObject(ctx context.Context, bucket string, key string) (*storage.Object, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".HeadObject()").End()

	return tsm.innerStorage.HeadObject(ctx, bucket, key)
}

func (tsm *tracingStorageMiddleware) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".GetObject()").End()

	return tsm.innerStorage.GetObject(ctx, bucket, key, startByte, endByte)
}

func (tsm *tracingStorageMiddleware) PutObject(ctx context.Context, bucket string, key string, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".PutObject()").End()

	return tsm.innerStorage.PutObject(ctx, bucket, key, contentType, reader, checksumInput)
}

func (tsm *tracingStorageMiddleware) DeleteObject(ctx context.Context, bucket string, key string) error {
	defer trace.StartRegion(ctx, tsm.regionName+".DeleteObject()").End()

	return tsm.innerStorage.DeleteObject(ctx, bucket, key)
}

func (tsm *tracingStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucket string, key string, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".CreateMultipartUpload()").End()

	return tsm.innerStorage.CreateMultipartUpload(ctx, bucket, key, contentType, checksumType)
}

func (tsm *tracingStorageMiddleware) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".UploadPart()").End()

	return tsm.innerStorage.UploadPart(ctx, bucket, key, uploadId, partNumber, reader, checksumInput)
}

func (tsm *tracingStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".CompleteMultipartUpload()").End()

	return tsm.innerStorage.CompleteMultipartUpload(ctx, bucket, key, uploadId, checksumInput)
}

func (tsm *tracingStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	defer trace.StartRegion(ctx, tsm.regionName+".AbortMultipartUpload()").End()

	return tsm.innerStorage.AbortMultipartUpload(ctx, bucket, key, uploadId)
}

func (tsm *tracingStorageMiddleware) ListMultipartUploads(ctx context.Context, bucket string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*storage.ListMultipartUploadsResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListMultipartUploads()").End()

	return tsm.innerStorage.ListMultipartUploads(ctx, bucket, prefix, delimiter, keyMarker, uploadIdMarker, maxUploads)
}

func (tsm *tracingStorageMiddleware) ListParts(ctx context.Context, bucket string, key string, uploadId string, partNumberMarker string, maxParts int32) (*storage.ListPartsResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListParts()").End()

	return tsm.innerStorage.ListParts(ctx, bucket, key, uploadId, partNumberMarker, maxParts)
}
