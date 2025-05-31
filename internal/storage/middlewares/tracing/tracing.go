package tracing

import (
	"context"
	"io"
	"runtime/trace"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/startstopvalidator"
)

type tracingStorageMiddleware struct {
	regionName         string
	innerStorage       storage.Storage
	startStopValidator *startstopvalidator.StartStopValidator
}

func NewStorageMiddleware(regionName string, innerStorage storage.Storage) (storage.Storage, error) {
	startStopValidator, err := startstopvalidator.New("TracingStorageMiddleware")
	if err != nil {
		return nil, err
	}

	return &tracingStorageMiddleware{
		regionName:         regionName,
		innerStorage:       innerStorage,
		startStopValidator: startStopValidator,
	}, nil
}

func (tsm *tracingStorageMiddleware) Start(ctx context.Context) error {
	defer trace.StartRegion(ctx, tsm.regionName+".Start()").End()

	err := tsm.startStopValidator.Start()
	if err != nil {
		return err
	}

	err = tsm.innerStorage.Start(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (tsm *tracingStorageMiddleware) Stop(ctx context.Context) error {
	defer trace.StartRegion(ctx, tsm.regionName+".Stop()").End()

	err := tsm.startStopValidator.Stop()
	if err != nil {
		return err
	}

	err = tsm.innerStorage.Stop(ctx)
	if err != nil {
		return err
	}
	return nil
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

func (tsm *tracingStorageMiddleware) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*storage.ListBucketResult, error) {
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

func (tsm *tracingStorageMiddleware) PutObject(ctx context.Context, bucket string, key string, contentType string, reader io.Reader) error {
	defer trace.StartRegion(ctx, tsm.regionName+".PutObject()").End()

	return tsm.innerStorage.PutObject(ctx, bucket, key, contentType, reader)
}

func (tsm *tracingStorageMiddleware) DeleteObject(ctx context.Context, bucket string, key string) error {
	defer trace.StartRegion(ctx, tsm.regionName+".DeleteObject()").End()

	return tsm.innerStorage.DeleteObject(ctx, bucket, key)
}

func (tsm *tracingStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucket string, key string, contentType string) (*storage.InitiateMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".CreateMultipartUpload()").End()

	return tsm.innerStorage.CreateMultipartUpload(ctx, bucket, key, contentType)
}

func (tsm *tracingStorageMiddleware) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, reader io.Reader) (*storage.UploadPartResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".UploadPart()").End()

	return tsm.innerStorage.UploadPart(ctx, bucket, key, uploadId, partNumber, reader)
}

func (tsm *tracingStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*storage.CompleteMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".CompleteMultipartUpload()").End()

	return tsm.innerStorage.CompleteMultipartUpload(ctx, bucket, key, uploadId)
}

func (tsm *tracingStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	defer trace.StartRegion(ctx, tsm.regionName+".AbortMultipartUpload()").End()

	return tsm.innerStorage.AbortMultipartUpload(ctx, bucket, key, uploadId)
}

func (tsm *tracingStorageMiddleware) ListMultipartUploads(ctx context.Context, bucket string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int) (*storage.ListMultipartUploadsResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListMultipartUploads()").End()

	return tsm.innerStorage.ListMultipartUploads(ctx, bucket, prefix, delimiter, keyMarker, uploadIdMarker, maxUploads)
}
