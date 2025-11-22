package cache

import (
	"context"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type CacheStorage struct {
	*lifecycle.ValidatedLifecycle
	cache        Cache
	innerStorage storage.Storage
	tracer       trace.Tracer
}

// Compile-time check to ensure CacheStorage implements storage.Storage
var _ storage.Storage = (*CacheStorage)(nil)

func New(cache Cache, innerStorage storage.Storage) (storage.Storage, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("CacheStorage")
	if err != nil {
		return nil, err
	}
	return &CacheStorage{
		ValidatedLifecycle: lifecycle,
		cache:              cache,
		innerStorage:       innerStorage,
		tracer:             otel.Tracer("internal/storage/cache"),
	}, nil
}

func getObjectCacheKeyForBucketAndKey(bucketName storage.BucketName, key storage.ObjectKey) string {
	return "OBJECT_BUCKET_" + bucketName.String() + "_KEY_" + key.String()
}

func (cs *CacheStorage) Start(ctx context.Context) error {
	if err := cs.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	return cs.innerStorage.Start(ctx)
}

func (cs *CacheStorage) Stop(ctx context.Context) error {
	if err := cs.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	return cs.innerStorage.Stop(ctx)
}

func (cs *CacheStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.CreateBucket")
	defer span.End()

	err := cs.innerStorage.CreateBucket(ctx, bucketName)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.DeleteBucket")
	defer span.End()

	err := cs.innerStorage.DeleteBucket(ctx, bucketName)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.ListBuckets")
	defer span.End()

	buckets, err := cs.innerStorage.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	return buckets, nil
}

func (cs *CacheStorage) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.HeadBucket")
	defer span.End()

	bucket, err := cs.innerStorage.HeadBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func (cs *CacheStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.ListObjects")
	defer span.End()

	objects, err := cs.innerStorage.ListObjects(ctx, bucketName, opts)
	if err != nil {
		return nil, err
	}
	return objects, nil
}

func (cs *CacheStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (*storage.Object, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.HeadObject")
	defer span.End()

	object, err := cs.innerStorage.HeadObject(ctx, bucketName, key)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (cs *CacheStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.GetObject")
	defer span.End()

	var reader io.ReadCloser
	cacheKey := getObjectCacheKeyForBucketAndKey(bucketName, key)
	data, err := cs.cache.Get(cacheKey)
	if err != nil && err != ErrCacheMiss {
		return nil, err
	}

	if err == ErrCacheMiss {
		// @TODO: only cache byteRange that was requested
		reader, err := cs.innerStorage.GetObject(ctx, bucketName, key, nil, nil)
		if err != nil {
			return nil, err
		}
		defer reader.Close()

		data, err = io.ReadAll(reader)
		if err != nil {
			return nil, err
		}

		err = cs.cache.Set(cacheKey, data)
		if err != nil {
			return nil, err
		}
	}
	reader = ioutils.NewByteReadSeekCloser(data)

	// We need to apply the LimitedEndReadSeekCloser first, otherwise we need to recalculate the end offset
	// because the LimitedStartSeekCloser changes the offsets
	if endByte != nil {
		reader = ioutils.NewLimitedEndReadCloser(reader, *endByte)
	}
	if startByte != nil {
		_, err := ioutils.SkipNBytes(reader, *startByte)
		if err != nil {
			return nil, err
		}
	}
	return reader, nil
}

func (cs *CacheStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.PutObject")
	defer span.End()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(data)

	putObjectResult, err := cs.innerStorage.PutObject(ctx, bucketName, key, contentType, byteReadSeekCloser, checksumInput)
	if err != nil {
		return nil, err
	}

	cacheKey := getObjectCacheKeyForBucketAndKey(bucketName, key)
	err = cs.cache.Set(cacheKey, data)
	if err != nil {
		return nil, err
	}

	return putObjectResult, nil
}

func (cs *CacheStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.DeleteObject")
	defer span.End()

	err := cs.innerStorage.DeleteObject(ctx, bucketName, key)
	if err != nil {
		return err
	}

	cacheKey := getObjectCacheKeyForBucketAndKey(bucketName, key)
	err = cs.cache.Remove(cacheKey)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.CreateMultipartUpload")
	defer span.End()

	initiateMultipartUploadResult, err := cs.innerStorage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
	if err != nil {
		return nil, err
	}
	return initiateMultipartUploadResult, nil
}

func (cs *CacheStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.UploadPart")
	defer span.End()

	uploadPartResult, err := cs.innerStorage.UploadPart(ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
	if err != nil {
		return nil, err
	}
	return uploadPartResult, nil
}

func (cs *CacheStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.CompleteMultipartUpload")
	defer span.End()

	completeMultipartUploadResult, err := cs.innerStorage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput)
	if err != nil {
		return nil, err
	}
	return completeMultipartUploadResult, nil
}

func (cs *CacheStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.AbortMultipartUpload")
	defer span.End()

	err := cs.innerStorage.AbortMultipartUpload(ctx, bucketName, key, uploadId)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.ListMultipartUploads")
	defer span.End()

	listMultipartUploadsResult, err := cs.innerStorage.ListMultipartUploads(ctx, bucketName, opts)
	if err != nil {
		return nil, err
	}
	return listMultipartUploadsResult, nil
}

func (cs *CacheStorage) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.ListParts")
	defer span.End()

	listPartsResult, err := cs.innerStorage.ListParts(ctx, bucketName, key, uploadId, opts)
	if err != nil {
		return nil, err
	}
	return listPartsResult, nil
}
