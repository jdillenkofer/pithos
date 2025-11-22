package tracing

import (
	"context"
	"io"
	"runtime/trace"

	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type tracingStorageMiddleware struct {
	*lifecycle.ValidatedLifecycle
	regionName   string
	innerStorage storage.Storage
	tracer       oteltrace.Tracer
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
		tracer:             otel.Tracer("github.com/jdillenkofer/pithos/internal/storage/middlewares/tracing"),
	}, nil
}

func (tsm *tracingStorageMiddleware) Start(ctx context.Context) error {
	defer trace.StartRegion(ctx, tsm.regionName+".Start()").End()
	ctx, span := tsm.tracer.Start(ctx, tsm.regionName+".Start")
	defer span.End()

	if err := tsm.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}

	return tsm.innerStorage.Start(ctx)
}

func (tsm *tracingStorageMiddleware) Stop(ctx context.Context) error {
	defer trace.StartRegion(ctx, tsm.regionName+".Stop()").End()
	ctx, span := tsm.tracer.Start(ctx, tsm.regionName+".Stop")
	defer span.End()

	if err := tsm.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}

	return tsm.innerStorage.Stop(ctx)
}

func (tsm *tracingStorageMiddleware) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	defer trace.StartRegion(ctx, tsm.regionName+".CreateBucket()").End()
	ctx, span := tsm.tracer.Start(ctx, tsm.regionName+".CreateBucket")
	defer span.End()

	return tsm.innerStorage.CreateBucket(ctx, bucketName)
}

func (tsm *tracingStorageMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	defer trace.StartRegion(ctx, tsm.regionName+".DeleteBucket()").End()
	ctx, span := tsm.tracer.Start(ctx, tsm.regionName+".DeleteBucket")
	defer span.End()

	return tsm.innerStorage.DeleteBucket(ctx, bucketName)
}

func (tsm *tracingStorageMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListBuckets()").End()
	ctx, span := tsm.tracer.Start(ctx, tsm.regionName+".ListBuckets")
	defer span.End()

	return tsm.innerStorage.ListBuckets(ctx)
}

func (tsm *tracingStorageMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".HeadBucket()").End()
	ctx, span := tsm.tracer.Start(ctx, tsm.regionName+".HeadBucket")
	defer span.End()

	return tsm.innerStorage.HeadBucket(ctx, bucketName)
}

func (tsm *tracingStorageMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListObjects()").End()
	ctx, span := tsm.tracer.Start(ctx, tsm.regionName+".ListObjects")
	defer span.End()

	return tsm.innerStorage.ListObjects(ctx, bucketName, opts)
}

func (tsm *tracingStorageMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (*storage.Object, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".HeadObject()").End()
	ctx, span := tsm.tracer.Start(ctx, tsm.regionName+".HeadObject")
	defer span.End()

	return tsm.innerStorage.HeadObject(ctx, bucketName, key)
}

func (tsm *tracingStorageMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".GetObject()").End()
	ctx, span := tsm.tracer.Start(ctx, tsm.regionName+".GetObject")
	defer span.End()

	return tsm.innerStorage.GetObject(ctx, bucketName, key, startByte, endByte)
}

func (tsm *tracingStorageMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".PutObject()").End()
	ctx, span := tsm.tracer.Start(ctx, tsm.regionName+".PutObject")
	defer span.End()

	return tsm.innerStorage.PutObject(ctx, bucketName, key, contentType, reader, checksumInput)
}

func (tsm *tracingStorageMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	defer trace.StartRegion(ctx, tsm.regionName+".DeleteObject()").End()

	return tsm.innerStorage.DeleteObject(ctx, bucketName, key)
}

func (tsm *tracingStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".CreateMultipartUpload()").End()

	return tsm.innerStorage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
}

func (tsm *tracingStorageMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".UploadPart()").End()

	return tsm.innerStorage.UploadPart(ctx, bucketName, key, uploadId, partNumber, reader, checksumInput)
}

func (tsm *tracingStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".CompleteMultipartUpload()").End()

	return tsm.innerStorage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput)
}

func (tsm *tracingStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	defer trace.StartRegion(ctx, tsm.regionName+".AbortMultipartUpload()").End()

	return tsm.innerStorage.AbortMultipartUpload(ctx, bucketName, key, uploadId)
}

func (tsm *tracingStorageMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListMultipartUploads()").End()

	return tsm.innerStorage.ListMultipartUploads(ctx, bucketName, opts)
}

func (tsm *tracingStorageMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListParts()").End()

	return tsm.innerStorage.ListParts(ctx, bucketName, key, uploadId, opts)
}
