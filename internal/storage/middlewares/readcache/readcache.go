package readcache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"

	cachepkg "github.com/jdillenkofer/pithos/internal/cache"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
)

const defaultMaxObjectSizeBytes int64 = 64 * 1024 * 1024

type Options struct {
	MaxObjectSizeBytes    int64
	CacheReadErrorsAsMiss bool
}

type readCacheStorageMiddleware struct {
	delegator.DelegatingStorage
	cache                 cachepkg.Cache
	maxObjectSizeBytes    int64
	cacheReadErrorsAsMiss bool
	tracer                trace.Tracer
	readGroup             singleflight.Group
}

type cachedObject struct {
	Object storage.Object `json:"object"`
	Body   []byte         `json:"body"`
}

var errObjectLargerThanCacheThreshold = errors.New("object larger than cache threshold")

var _ storage.Storage = (*readCacheStorageMiddleware)(nil)

func NewStorageMiddleware(innerStorage storage.Storage, cache cachepkg.Cache, opts Options) (storage.Storage, error) {
	maxObjectSizeBytes := opts.MaxObjectSizeBytes
	if maxObjectSizeBytes <= 0 {
		maxObjectSizeBytes = defaultMaxObjectSizeBytes
	}
	return &readCacheStorageMiddleware{
		DelegatingStorage:     delegator.Wrap(innerStorage),
		cache:                 cache,
		maxObjectSizeBytes:    maxObjectSizeBytes,
		cacheReadErrorsAsMiss: opts.CacheReadErrorsAsMiss,
		tracer:                otel.Tracer("internal/storage/middlewares/readcache"),
	}, nil
}

func objectCacheKey(bucketName storage.BucketName, key storage.ObjectKey) string {
	return fmt.Sprintf("READCACHE_OBJECT_%s_%s", bucketName.String(), key.String())
}

func headCacheKey(bucketName storage.BucketName, key storage.ObjectKey) string {
	return fmt.Sprintf("READCACHE_HEAD_%s_%s", bucketName.String(), key.String())
}

func (m *readCacheStorageMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	ctx, span := m.tracer.Start(ctx, "ReadCacheStorageMiddleware.HeadObject")
	defer span.End()

	headKey := headCacheKey(bucketName, key)
	if obj, err := m.readHeadFromCache(ctx, headKey); err == nil {
		if err = validateConditionalHead(obj, opts); err != nil {
			return nil, err
		}
		return cloneObject(obj), nil
	}

	objKey := objectCacheKey(bucketName, key)
	if cachedObj, err := m.readObjectFromCache(ctx, objKey); err == nil {
		if err = validateConditionalHead(&cachedObj.Object, opts); err != nil {
			return nil, err
		}
		if err = m.writeHeadToCache(ctx, headKey, &cachedObj.Object); err != nil {
			slog.DebugContext(ctx, "Failed to write head cache from object cache", "key", headKey, "error", err)
		}
		return cloneObject(&cachedObj.Object), nil
	}
	v, err, _ := m.readGroup.Do(headKey, func() (interface{}, error) {
		cachedHead, cacheErr := m.readHeadFromCache(ctx, headKey)
		if cacheErr == nil {
			return cachedHead, nil
		}

		obj, innerErr := m.Next.HeadObject(ctx, bucketName, key, nil)
		if innerErr != nil {
			return nil, innerErr
		}
		if writeErr := m.writeHeadToCache(ctx, headKey, obj); writeErr != nil {
			slog.DebugContext(ctx, "Failed to write head cache", "key", headKey, "error", writeErr)
		}
		return obj, nil
	})
	if err != nil {
		return nil, err
	}
	obj, ok := v.(*storage.Object)
	if !ok {
		return nil, fmt.Errorf("invalid head cache payload type")
	}
	if err = validateConditionalHead(obj, opts); err != nil {
		return nil, err
	}
	return cloneObject(obj), nil
}

func (m *readCacheStorageMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := m.tracer.Start(ctx, "ReadCacheStorageMiddleware.GetObject")
	defer span.End()

	if len(ranges) > 0 {
		return m.Next.GetObject(ctx, bucketName, key, ranges, opts)
	}

	objKey := objectCacheKey(bucketName, key)
	if cachedObj, err := m.readObjectFromCache(ctx, objKey); err == nil {
		if err = validateConditionalGet(&cachedObj.Object, opts); err != nil {
			return nil, nil, err
		}
		return cloneObject(&cachedObj.Object), []io.ReadCloser{io.NopCloser(bytes.NewReader(cachedObj.Body))}, nil
	}
	v, err, _ := m.readGroup.Do(objKey, func() (interface{}, error) {
		cachedObj, cacheErr := m.readObjectFromCache(ctx, objKey)
		if cacheErr == nil {
			return cachedObj, nil
		}

		obj, readers, innerErr := m.Next.GetObject(ctx, bucketName, key, nil, nil)
		if innerErr != nil {
			return nil, innerErr
		}
		if len(readers) != 1 {
			for _, r := range readers {
				_ = r.Close()
			}
			return nil, fmt.Errorf("expected single reader for full object fetch")
		}
		if obj.Size > m.maxObjectSizeBytes {
			_ = readers[0].Close()
			return nil, errObjectLargerThanCacheThreshold
		}

		body, readErr := io.ReadAll(readers[0])
		closeErr := readers[0].Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}

		entry := cachedObject{Object: *obj, Body: body}
		if writeErr := m.writeObjectToCache(ctx, objKey, &entry); writeErr != nil {
			slog.DebugContext(ctx, "Failed to write object cache", "key", objKey, "error", writeErr)
		}
		if writeErr := m.writeHeadToCache(ctx, headCacheKey(bucketName, key), obj); writeErr != nil {
			slog.DebugContext(ctx, "Failed to write head cache", "key", headCacheKey(bucketName, key), "error", writeErr)
		}
		return &entry, nil
	})
	if err != nil {
		if errors.Is(err, errObjectLargerThanCacheThreshold) {
			return m.Next.GetObject(ctx, bucketName, key, nil, opts)
		}
		return nil, nil, err
	}
	entry, ok := v.(*cachedObject)
	if !ok {
		return nil, nil, fmt.Errorf("invalid object cache payload type")
	}
	if err = validateConditionalGet(&entry.Object, opts); err != nil {
		return nil, nil, err
	}
	return cloneObject(&entry.Object), []io.ReadCloser{io.NopCloser(bytes.NewReader(entry.Body))}, nil
}

func (m *readCacheStorageMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	result, err := m.Next.PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	m.invalidateObjectCaches(ctx, bucketName, key)
	return result, nil
}

func (m *readCacheStorageMiddleware) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	result, err := m.Next.AppendObject(ctx, bucketName, key, data, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	m.invalidateObjectCaches(ctx, bucketName, key)
	return result, nil
}

func (m *readCacheStorageMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) error {
	err := m.Next.DeleteObject(ctx, bucketName, key, opts)
	if err != nil {
		return err
	}
	m.invalidateObjectCaches(ctx, bucketName, key)
	return nil
}

func (m *readCacheStorageMiddleware) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	result, err := m.Next.DeleteObjects(ctx, bucketName, entries)
	if err != nil {
		return nil, err
	}
	for _, entry := range result.Entries {
		if entry.Deleted {
			m.invalidateObjectCaches(ctx, bucketName, entry.Key)
		}
	}
	return result, nil
}

func (m *readCacheStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	result, err := m.Next.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	m.invalidateObjectCaches(ctx, bucketName, key)
	return result, nil
}

func (m *readCacheStorageMiddleware) invalidateObjectCaches(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) {
	objKey := objectCacheKey(bucketName, key)
	if err := m.cache.Remove(objKey); err != nil {
		slog.DebugContext(ctx, "Failed to remove object cache key", "key", objKey, "error", err)
	}
	headKey := headCacheKey(bucketName, key)
	if err := m.cache.Remove(headKey); err != nil {
		slog.DebugContext(ctx, "Failed to remove head cache key", "key", headKey, "error", err)
	}
}

func (m *readCacheStorageMiddleware) readHeadFromCache(ctx context.Context, key string) (*storage.Object, error) {
	bytesData, err := m.cache.Get(key)
	if err != nil {
		if err != cachepkg.ErrCacheMiss && !m.cacheReadErrorsAsMiss {
			return nil, err
		}
		return nil, cachepkg.ErrCacheMiss
	}
	var obj storage.Object
	if err = json.Unmarshal(bytesData, &obj); err != nil {
		_ = m.cache.Remove(key)
		slog.DebugContext(ctx, "Failed to decode head cache entry", "key", key, "error", err)
		return nil, cachepkg.ErrCacheMiss
	}
	return &obj, nil
}

func (m *readCacheStorageMiddleware) readObjectFromCache(ctx context.Context, key string) (*cachedObject, error) {
	bytesData, err := m.cache.Get(key)
	if err != nil {
		if err != cachepkg.ErrCacheMiss && !m.cacheReadErrorsAsMiss {
			return nil, err
		}
		return nil, cachepkg.ErrCacheMiss
	}
	var obj cachedObject
	if err = json.Unmarshal(bytesData, &obj); err != nil {
		_ = m.cache.Remove(key)
		slog.DebugContext(ctx, "Failed to decode object cache entry", "key", key, "error", err)
		return nil, cachepkg.ErrCacheMiss
	}
	return &obj, nil
}

func (m *readCacheStorageMiddleware) writeHeadToCache(ctx context.Context, key string, obj *storage.Object) error {
	bytesData, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	err = m.cache.Set(key, bytesData)
	if err != nil {
		slog.DebugContext(ctx, "Failed to write head cache entry", "key", key, "error", err)
	}
	return err
}

func (m *readCacheStorageMiddleware) writeObjectToCache(ctx context.Context, key string, obj *cachedObject) error {
	bytesData, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	err = m.cache.Set(key, bytesData)
	if err != nil {
		slog.DebugContext(ctx, "Failed to write object cache entry", "key", key, "error", err)
	}
	return err
}

func validateConditionalHead(obj *storage.Object, opts *storage.HeadObjectOptions) error {
	if opts == nil {
		return nil
	}
	if opts.IfMatchETag != nil && *opts.IfMatchETag != storage.ETagWildcard && obj.ETag != *opts.IfMatchETag {
		return storage.ErrPreconditionFailed
	}
	if opts.IfNoneMatchETag != nil {
		if *opts.IfNoneMatchETag == storage.ETagWildcard || obj.ETag == *opts.IfNoneMatchETag {
			return storage.ErrNotModified
		}
	}
	return nil
}

func validateConditionalGet(obj *storage.Object, opts *storage.GetObjectOptions) error {
	if opts == nil {
		return nil
	}
	if opts.IfMatchETag != nil && *opts.IfMatchETag != storage.ETagWildcard && obj.ETag != *opts.IfMatchETag {
		return storage.ErrPreconditionFailed
	}
	if opts.IfNoneMatchETag != nil {
		if *opts.IfNoneMatchETag == storage.ETagWildcard || obj.ETag == *opts.IfNoneMatchETag {
			return storage.ErrNotModified
		}
	}
	return nil
}

func cloneObject(obj *storage.Object) *storage.Object {
	if obj == nil {
		return nil
	}
	clone := *obj
	return &clone
}
