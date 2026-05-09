package cache

import (
	"context"
	"fmt"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
)

type CacheStorage struct {
	*lifecycle.ValidatedLifecycle
	delegator.DelegatingStorage
	cache  Cache
	tracer trace.Tracer
}

var _ storage.Storage = (*CacheStorage)(nil)

func New(cache Cache, innerStorage storage.Storage) (storage.Storage, error) {
	lc, err := lifecycle.NewValidatedLifecycle("CacheStorage")
	if err != nil {
		return nil, err
	}
	return &CacheStorage{
		ValidatedLifecycle: lc,
		DelegatingStorage:  delegator.Wrap(innerStorage),
		cache:              cache,
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
	return cs.Next.Start(ctx)
}

func (cs *CacheStorage) Stop(ctx context.Context) error {
	if err := cs.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	return cs.Next.Stop(ctx)
}

func (cs *CacheStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.GetObject")
	defer span.End()

	if len(ranges) == 0 {
		ranges = []storage.ByteRange{{Start: nil, End: nil}}
	}

	object, err := cs.Next.HeadObject(ctx, bucketName, key, nil)
	if err != nil {
		return nil, nil, err
	}

	cacheKey := getObjectCacheKeyForBucketAndKey(bucketName, key)
	data, err := cs.cache.Get(cacheKey)
	if err != nil && err != ErrCacheMiss {
		return nil, nil, err
	}

	if err == ErrCacheMiss {
		_, readers, err := cs.Next.GetObject(ctx, bucketName, key, nil, opts)
		if err != nil {
			return nil, nil, err
		}
		if len(readers) == 0 {
			return nil, nil, fmt.Errorf("no readers returned")
		}
		reader := readers[0]
		defer reader.Close()

		data, err = io.ReadAll(reader)
		if err != nil {
			return nil, nil, err
		}

		err = cs.cache.Set(cacheKey, data)
		if err != nil {
			return nil, nil, err
		}
	}

	readers := []io.ReadCloser{}
	for _, byteRange := range ranges {
		startByte := byteRange.Start
		endByte := byteRange.End

		var reader io.ReadCloser = ioutils.NewByteReadSeekCloser(data)
		if endByte != nil {
			reader = ioutils.NewLimitedEndReadCloser(reader, *endByte)
		}
		if startByte != nil {
			_, err := ioutils.SkipNBytes(reader, *startByte)
			if err != nil {
				for _, r := range readers {
					r.Close()
				}
				return nil, nil, err
			}
		}
		readers = append(readers, reader)
	}

	return object, readers, nil
}

func (cs *CacheStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.PutObject")
	defer span.End()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(data)

	putObjectResult, err := cs.Next.PutObject(ctx, bucketName, key, contentType, byteReadSeekCloser, checksumInput, opts)
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

func (cs *CacheStorage) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.AppendObject")
	defer span.End()

	appendObjectResult, err := cs.Next.AppendObject(ctx, bucketName, key, reader, checksumInput, opts)
	if err != nil {
		return nil, err
	}

	cacheKey := getObjectCacheKeyForBucketAndKey(bucketName, key)
	_ = cs.cache.Remove(cacheKey)

	return appendObjectResult, nil
}

func (cs *CacheStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) error {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.DeleteObject")
	defer span.End()

	err := cs.Next.DeleteObject(ctx, bucketName, key, opts)
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

func (cs *CacheStorage) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	ctx, span := cs.tracer.Start(ctx, "CacheStorage.DeleteObjects")
	defer span.End()

	result, err := cs.Next.DeleteObjects(ctx, bucketName, entries)
	if err != nil {
		return nil, err
	}

	for _, entry := range result.Entries {
		if entry.Deleted {
			cacheKey := getObjectCacheKeyForBucketAndKey(bucketName, entry.Key)
			_ = cs.cache.Remove(cacheKey)
		}
	}

	return result, nil
}
