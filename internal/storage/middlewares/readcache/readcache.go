package readcache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"

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
}

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
	return fmt.Sprintf("READCACHE_OBJECT_BODY_%s_%s", bucketName.String(), key.String())
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
		if err = validateConditionalHead(cachedObj, opts); err != nil {
			return nil, err
		}
		if err = m.writeHeadToCache(ctx, headKey, cachedObj); err != nil {
			slog.DebugContext(ctx, "Failed to write head cache from object cache", "key", headKey, "error", err)
		}
		return cloneObject(cachedObj), nil
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
		if err = validateConditionalGet(cachedObj, opts); err != nil {
			return nil, nil, err
		}
		bodyReader, getErr := m.cache.Get(objKey)
		if getErr == nil {
			return cloneObject(cachedObj), []io.ReadCloser{bodyReader}, nil
		}
		if getErr != cachepkg.ErrCacheMiss && !m.cacheReadErrorsAsMiss {
			return nil, nil, getErr
		}
	}

	obj, readers, err := m.Next.GetObject(ctx, bucketName, key, nil, opts)
	if err != nil {
		return nil, nil, err
	}
	if len(readers) != 1 {
		return obj, readers, nil
	}
	if obj.Size > m.maxObjectSizeBytes {
		return obj, readers, nil
	}
	if err = m.writeHeadToCache(ctx, headCacheKey(bucketName, key), obj); err != nil {
		slog.DebugContext(ctx, "Failed to write head cache", "key", headCacheKey(bucketName, key), "error", err)
	}

	pr, pw := io.Pipe()
	cacheWriteDone := make(chan struct{})
	go func(cacheKey string, objectSize int64) {
		defer close(cacheWriteDone)
		setErr := m.cache.Set(cacheKey, pr, objectSize)
		if setErr != nil {
			slog.DebugContext(ctx, "Failed to write streamed object body to cache", "key", cacheKey, "error", setErr)
		}
	}(objKey, obj.Size)

	return cloneObject(obj), []io.ReadCloser{&cacheOnReadCloser{
		ReadCloser:     readers[0],
		pipeWriter:     pw,
		cacheWriteDone: cacheWriteDone,
	}}, nil
}

type cacheOnReadCloser struct {
	io.ReadCloser
	pipeWriter     *io.PipeWriter
	cacheWriteDone chan struct{}
	closed         bool
}

func (r *cacheOnReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		if _, writeErr := r.pipeWriter.Write(p[:n]); writeErr != nil {
			_ = r.pipeWriter.CloseWithError(writeErr)
			r.pipeWriter = nil
		}
	}
	if err == io.EOF {
		if r.pipeWriter != nil {
			_ = r.pipeWriter.Close()
			r.pipeWriter = nil
		}
	} else if err != nil {
		if r.pipeWriter != nil {
			_ = r.pipeWriter.CloseWithError(err)
			r.pipeWriter = nil
		}
	}
	return n, err
}

func (r *cacheOnReadCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.pipeWriter != nil {
		_ = r.pipeWriter.CloseWithError(io.ErrUnexpectedEOF)
		r.pipeWriter = nil
	}
	if r.cacheWriteDone != nil {
		<-r.cacheWriteDone
		r.cacheWriteDone = nil
	}
	return r.ReadCloser.Close()
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
	rc, err := m.cache.Get(key)
	if err != nil {
		if err != cachepkg.ErrCacheMiss && !m.cacheReadErrorsAsMiss {
			return nil, err
		}
		return nil, cachepkg.ErrCacheMiss
	}
	var obj storage.Object
	if err = json.NewDecoder(rc).Decode(&obj); err != nil {
		_ = rc.Close()
		_ = m.cache.Remove(key)
		slog.DebugContext(ctx, "Failed to decode head cache entry", "key", key, "error", err)
		return nil, cachepkg.ErrCacheMiss
	}
	if err = rc.Close(); err != nil {
		return nil, cachepkg.ErrCacheMiss
	}
	return &obj, nil
}

func (m *readCacheStorageMiddleware) readObjectFromCache(ctx context.Context, key string) (*storage.Object, error) {
	headKey := strings.Replace(key, "READCACHE_OBJECT_BODY_", "READCACHE_HEAD_", 1)
	obj, err := m.readHeadFromCache(ctx, headKey)
	if err != nil {
		return nil, err
	}
	rc, err := m.cache.Get(key)
	if err != nil {
		if err != cachepkg.ErrCacheMiss && !m.cacheReadErrorsAsMiss {
			return nil, err
		}
		return nil, cachepkg.ErrCacheMiss
	}
	if err = rc.Close(); err != nil {
		return nil, cachepkg.ErrCacheMiss
	}
	return obj, nil
}

func (m *readCacheStorageMiddleware) writeHeadToCache(ctx context.Context, key string, obj *storage.Object) error {
	bytesData, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	err = m.cache.Set(key, bytes.NewReader(bytesData), int64(len(bytesData)))
	if err != nil {
		slog.DebugContext(ctx, "Failed to write head cache entry", "key", key, "error", err)
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
