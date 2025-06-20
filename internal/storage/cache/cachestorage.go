package cache

import (
	"context"
	"io"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/startstopvalidator"
)

type CacheStorage struct {
	cache              Cache
	innerStorage       storage.Storage
	startStopValidator *startstopvalidator.StartStopValidator
}

func New(cache Cache, innerStorage storage.Storage) (storage.Storage, error) {
	startStopValidator, err := startstopvalidator.New("CacheStorage")
	if err != nil {
		return nil, err
	}
	return &CacheStorage{
		cache:              cache,
		innerStorage:       innerStorage,
		startStopValidator: startStopValidator,
	}, nil
}

func getObjectCacheKeyForBucketAndKey(bucket string, key string) string {
	return "OBJECT_BUCKET_" + bucket + "_KEY_" + key
}

func (cs *CacheStorage) Start(ctx context.Context) error {
	err := cs.startStopValidator.Start()
	if err != nil {
		return err
	}
	err = cs.innerStorage.Start(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) Stop(ctx context.Context) error {
	err := cs.startStopValidator.Stop()
	if err != nil {
		return err
	}
	err = cs.innerStorage.Stop(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) CreateBucket(ctx context.Context, bucket string) error {
	err := cs.innerStorage.CreateBucket(ctx, bucket)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) DeleteBucket(ctx context.Context, bucket string) error {
	err := cs.innerStorage.DeleteBucket(ctx, bucket)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	buckets, err := cs.innerStorage.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	return buckets, nil
}

func (cs *CacheStorage) HeadBucket(ctx context.Context, bucketName string) (*storage.Bucket, error) {
	bucket, err := cs.innerStorage.HeadBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func (cs *CacheStorage) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int32) (*storage.ListBucketResult, error) {
	objects, err := cs.innerStorage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
	if err != nil {
		return nil, err
	}
	return objects, nil
}

func (cs *CacheStorage) HeadObject(ctx context.Context, bucket string, key string) (*storage.Object, error) {
	object, err := cs.innerStorage.HeadObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (cs *CacheStorage) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	var reader io.ReadCloser
	cacheKey := getObjectCacheKeyForBucketAndKey(bucket, key)
	data, err := cs.cache.Get(cacheKey)
	if err != nil && err != ErrCacheMiss {
		return nil, err
	}

	if err == ErrCacheMiss {
		// @TODO: only cache byteRange that was requested
		reader, err := cs.innerStorage.GetObject(ctx, bucket, key, nil, nil)
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

func (cs *CacheStorage) PutObject(ctx context.Context, bucket string, key string, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(data)

	putObjectResult, err := cs.innerStorage.PutObject(ctx, bucket, key, contentType, byteReadSeekCloser, checksumInput)
	if err != nil {
		return nil, err
	}

	cacheKey := getObjectCacheKeyForBucketAndKey(bucket, key)
	err = cs.cache.Set(cacheKey, data)
	if err != nil {
		return nil, err
	}

	return putObjectResult, nil
}

func (cs *CacheStorage) DeleteObject(ctx context.Context, bucket string, key string) error {
	err := cs.innerStorage.DeleteObject(ctx, bucket, key)
	if err != nil {
		return err
	}

	cacheKey := getObjectCacheKeyForBucketAndKey(bucket, key)
	err = cs.cache.Remove(cacheKey)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) CreateMultipartUpload(ctx context.Context, bucket string, key string, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	initiateMultipartUploadResult, err := cs.innerStorage.CreateMultipartUpload(ctx, bucket, key, contentType, checksumType)
	if err != nil {
		return nil, err
	}
	return initiateMultipartUploadResult, nil
}

func (cs *CacheStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	uploadPartResult, err := cs.innerStorage.UploadPart(ctx, bucket, key, uploadId, partNumber, data, checksumInput)
	if err != nil {
		return nil, err
	}
	return uploadPartResult, nil
}

func (cs *CacheStorage) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	completeMultipartUploadResult, err := cs.innerStorage.CompleteMultipartUpload(ctx, bucket, key, uploadId, checksumInput)
	if err != nil {
		return nil, err
	}
	return completeMultipartUploadResult, nil
}

func (cs *CacheStorage) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	err := cs.innerStorage.AbortMultipartUpload(ctx, bucket, key, uploadId)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) ListMultipartUploads(ctx context.Context, bucket string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*storage.ListMultipartUploadsResult, error) {
	listMultipartUploadsResult, err := cs.innerStorage.ListMultipartUploads(ctx, bucket, prefix, delimiter, keyMarker, uploadIdMarker, maxUploads)
	if err != nil {
		return nil, err
	}
	return listMultipartUploadsResult, nil
}

func (cs *CacheStorage) ListParts(ctx context.Context, bucket string, key string, uploadId string, partNumberMarker string, maxParts int32) (*storage.ListPartsResult, error) {
	listPartsResult, err := cs.innerStorage.ListParts(ctx, bucket, key, uploadId, partNumberMarker, maxParts)
	if err != nil {
		return nil, err
	}
	return listPartsResult, nil
}
