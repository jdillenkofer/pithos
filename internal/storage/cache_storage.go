package storage

import (
	"context"
	"io"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/cache"
)

type CacheStorage struct {
	cache        cache.Cache
	innerStorage Storage
}

func NewCacheStorage(cache cache.Cache, innerStorage Storage) (*CacheStorage, error) {
	return &CacheStorage{
		cache:        cache,
		innerStorage: innerStorage,
	}, nil
}

func getObjectCacheKeyForBucketAndKey(bucket string, key string) string {
	return "OBJECT_BUCKET_" + bucket + "_KEY_" + key
}

func (cs *CacheStorage) Start(ctx context.Context) error {
	err := cs.innerStorage.Start(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) Stop(ctx context.Context) error {
	err := cs.innerStorage.Stop(ctx)
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

func (cs *CacheStorage) ListBuckets(ctx context.Context) ([]Bucket, error) {
	buckets, err := cs.innerStorage.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	return buckets, nil
}

func (cs *CacheStorage) HeadBucket(ctx context.Context, bucketName string) (*Bucket, error) {
	bucket, err := cs.innerStorage.HeadBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func (cs *CacheStorage) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	objects, err := cs.innerStorage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
	if err != nil {
		return nil, err
	}
	return objects, nil
}

func (cs *CacheStorage) HeadObject(ctx context.Context, bucket string, key string) (*Object, error) {
	object, err := cs.innerStorage.HeadObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (cs *CacheStorage) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	var reader io.ReadSeekCloser
	cacheKey := getObjectCacheKeyForBucketAndKey(bucket, key)
	data, err := cs.cache.GetKey(cacheKey)
	if err != nil && err != cache.ErrKeyNotInCache {
		return nil, err
	}

	if err == cache.ErrKeyNotInCache {
		reader, err := cs.innerStorage.GetObject(ctx, bucket, key, nil, nil)
		if err != nil {
			return nil, err
		}
		defer reader.Close()

		data, err = io.ReadAll(reader)
		if err != nil {
			return nil, err
		}

		err = cs.cache.PutKey(cacheKey, data)
		if err != nil {
			return nil, err
		}
	}
	reader = ioutils.NewByteReadSeekCloser(data)

	// We need to apply the LimitedEndReadSeekCloser first, otherwise we need to recalculate the end offset
	// because the LimitedStartSeekCloser changes the offsets
	if endByte != nil {
		reader = ioutils.NewLimitedEndReadSeekCloser(reader, *endByte)
	}
	if startByte != nil {
		reader = ioutils.NewLimitedStartReadSeekCloser(reader, *startByte)
	}
	return reader, nil
}

func (cs *CacheStorage) PutObject(ctx context.Context, bucket string, key string, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(data)

	err = cs.innerStorage.PutObject(ctx, bucket, key, byteReadSeekCloser)
	if err != nil {
		return err
	}

	cacheKey := getObjectCacheKeyForBucketAndKey(bucket, key)
	err = cs.cache.PutKey(cacheKey, data)
	if err != nil {
		return err
	}

	return nil
}

func (cs *CacheStorage) DeleteObject(ctx context.Context, bucket string, key string) error {
	err := cs.innerStorage.DeleteObject(ctx, bucket, key)
	if err != nil {
		return err
	}

	cacheKey := getObjectCacheKeyForBucketAndKey(bucket, key)
	err = cs.cache.DeleteKey(cacheKey)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CacheStorage) CreateMultipartUpload(ctx context.Context, bucket string, key string) (*InitiateMultipartUploadResult, error) {
	initiateMultipartUploadResult, err := cs.innerStorage.CreateMultipartUpload(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	return initiateMultipartUploadResult, nil
}

func (cs *CacheStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, data io.Reader) (*UploadPartResult, error) {
	uploadPartResult, err := cs.innerStorage.UploadPart(ctx, bucket, key, uploadId, partNumber, data)
	if err != nil {
		return nil, err
	}
	return uploadPartResult, nil
}

func (cs *CacheStorage) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	completeMultipartUploadResult, err := cs.innerStorage.CompleteMultipartUpload(ctx, bucket, key, uploadId)
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
