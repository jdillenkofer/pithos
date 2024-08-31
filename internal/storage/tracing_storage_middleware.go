package storage

import (
	"context"
	"io"
	"runtime/trace"
)

type TracingStorageMiddleware struct {
	regionName   string
	innerStorage Storage
}

func NewTracingStorageMiddleware(regionName string, primaryStorage Storage) (*TracingStorageMiddleware, error) {
	return &TracingStorageMiddleware{
		regionName:   regionName,
		innerStorage: primaryStorage,
	}, nil
}

func (tsm *TracingStorageMiddleware) Start(ctx context.Context) error {
	defer trace.StartRegion(ctx, tsm.regionName+".Start()").End()

	return tsm.innerStorage.Start(ctx)
}

func (tsm *TracingStorageMiddleware) Stop(ctx context.Context) error {
	defer trace.StartRegion(ctx, tsm.regionName+".Stop()").End()

	return tsm.innerStorage.Stop(ctx)
}

func (tsm *TracingStorageMiddleware) CreateBucket(ctx context.Context, bucket string) error {
	defer trace.StartRegion(ctx, tsm.regionName+".CreateBucket()").End()

	return tsm.innerStorage.CreateBucket(ctx, bucket)
}

func (tsm *TracingStorageMiddleware) DeleteBucket(ctx context.Context, bucket string) error {
	defer trace.StartRegion(ctx, tsm.regionName+".DeleteBucket()").End()

	return tsm.innerStorage.DeleteBucket(ctx, bucket)
}

func (tsm *TracingStorageMiddleware) ListBuckets(ctx context.Context) ([]Bucket, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListBuckets()").End()

	return tsm.innerStorage.ListBuckets(ctx)
}

func (tsm *TracingStorageMiddleware) HeadBucket(ctx context.Context, bucket string) (*Bucket, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".HeadBucket()").End()

	return tsm.innerStorage.HeadBucket(ctx, bucket)
}

func (tsm *TracingStorageMiddleware) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".ListObjects()").End()

	return tsm.innerStorage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
}

func (tsm *TracingStorageMiddleware) HeadObject(ctx context.Context, bucket string, key string) (*Object, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".HeadObject()").End()

	return tsm.innerStorage.HeadObject(ctx, bucket, key)
}

func (tsm *TracingStorageMiddleware) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".GetObject()").End()

	return tsm.innerStorage.GetObject(ctx, bucket, key, startByte, endByte)
}

func (tsm *TracingStorageMiddleware) PutObject(ctx context.Context, bucket string, key string, reader io.Reader) error {
	defer trace.StartRegion(ctx, tsm.regionName+".PutObject()").End()

	return tsm.innerStorage.PutObject(ctx, bucket, key, reader)
}

func (tsm *TracingStorageMiddleware) DeleteObject(ctx context.Context, bucket string, key string) error {
	defer trace.StartRegion(ctx, tsm.regionName+".DeleteObject()").End()

	return tsm.innerStorage.DeleteObject(ctx, bucket, key)
}

func (tsm *TracingStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucket string, key string) (*InitiateMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".CreateMultipartUpload()").End()

	return tsm.innerStorage.CreateMultipartUpload(ctx, bucket, key)
}

func (tsm *TracingStorageMiddleware) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, reader io.Reader) (*UploadPartResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".UploadPart()").End()

	return tsm.innerStorage.UploadPart(ctx, bucket, key, uploadId, partNumber, reader)
}

func (tsm *TracingStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tsm.regionName+".CompleteMultipartUpload()").End()

	return tsm.innerStorage.CompleteMultipartUpload(ctx, bucket, key, uploadId)
}

func (tsm *TracingStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	defer trace.StartRegion(ctx, tsm.regionName+".AbortMultipartUpload()").End()

	return tsm.innerStorage.AbortMultipartUpload(ctx, bucket, key, uploadId)
}
