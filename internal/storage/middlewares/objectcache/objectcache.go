package objectcache

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"

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

type objectCacheStorageMiddleware struct {
	delegator.DelegatingStorage
	cache                 cachepkg.Cache
	maxObjectSizeBytes    int64
	cacheReadErrorsAsMiss bool
	tracer                trace.Tracer
	readGroup             singleflight.Group
	inflightMu            sync.Mutex
	inflightGets          map[string]*inflightGet
}

type inflightGet struct {
	done chan struct{}
	err  error
}

var errObjectLargerThanCacheThreshold = errors.New("object larger than cache threshold")

var _ storage.Storage = (*objectCacheStorageMiddleware)(nil)
var _ storage.TransactionalStorage = (*objectCacheStorageMiddleware)(nil)

func (m *objectCacheStorageMiddleware) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return delegator.WithTransaction(ctx, opts, m.Next, m, fn)
}

func NewStorageMiddleware(innerStorage storage.Storage, cache cachepkg.Cache, opts Options) (storage.Storage, error) {
	maxObjectSizeBytes := opts.MaxObjectSizeBytes
	if maxObjectSizeBytes <= 0 {
		maxObjectSizeBytes = defaultMaxObjectSizeBytes
	}
	return &objectCacheStorageMiddleware{
		DelegatingStorage:     delegator.Wrap(innerStorage),
		cache:                 cache,
		maxObjectSizeBytes:    maxObjectSizeBytes,
		cacheReadErrorsAsMiss: opts.CacheReadErrorsAsMiss,
		tracer:                otel.Tracer("internal/storage/middlewares/objectcache"),
		inflightGets:          map[string]*inflightGet{},
	}, nil
}

func objectCacheKey(bucketName storage.BucketName, key storage.ObjectKey) string {
	return fmt.Sprintf("OBJECTCACHE_OBJECT_BODY_%s_%s", bucketName.String(), key.String())
}

func headCacheKey(bucketName storage.BucketName, key storage.ObjectKey) string {
	return fmt.Sprintf("OBJECTCACHE_HEAD_%s_%s", bucketName.String(), key.String())
}

func (m *objectCacheStorageMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	ctx, span := m.tracer.Start(ctx, "ObjectCacheStorageMiddleware.HeadObject")
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

func (m *objectCacheStorageMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := m.tracer.Start(ctx, "ObjectCacheStorageMiddleware.GetObject")
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

	inflight, leader := m.getOrCreateInflightGet(objKey)
	if !leader {
		<-inflight.done
		if inflight.err != nil {
			return nil, nil, inflight.err
		}
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
	}

	if cachedObj, err := m.readObjectFromCache(ctx, objKey); err == nil {
		m.finishInflightGet(objKey, inflight, nil)
		inflight = nil
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
		m.finishInflightGet(objKey, inflight, err)
		inflight = nil
		return nil, nil, err
	}
	if len(readers) != 1 {
		m.finishInflightGet(objKey, inflight, nil)
		inflight = nil
		return obj, readers, nil
	}
	if obj.Size > m.maxObjectSizeBytes {
		m.finishInflightGet(objKey, inflight, nil)
		inflight = nil
		return obj, readers, nil
	}
	if err = m.writeHeadToCache(ctx, headCacheKey(bucketName, key), obj); err != nil {
		slog.DebugContext(ctx, "Failed to write head cache", "key", headCacheKey(bucketName, key), "error", err)
		m.finishInflightGet(objKey, inflight, nil)
		inflight = nil
		return obj, readers, nil
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
		onClose: func() {
			m.finishInflightGet(objKey, inflight, nil)
			inflight = nil
		},
	}}, nil
}

type cacheOnReadCloser struct {
	io.ReadCloser
	pipeWriter     *io.PipeWriter
	cacheWriteDone chan struct{}
	closed         bool
	onClose        func()
}

func (r *cacheOnReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 && r.pipeWriter != nil {
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
	if r.onClose != nil {
		r.onClose()
		r.onClose = nil
	}
	return r.ReadCloser.Close()
}

func (m *objectCacheStorageMiddleware) getOrCreateInflightGet(cacheKey string) (*inflightGet, bool) {
	m.inflightMu.Lock()
	defer m.inflightMu.Unlock()
	if inflight, ok := m.inflightGets[cacheKey]; ok {
		return inflight, false
	}
	inflight := &inflightGet{done: make(chan struct{})}
	m.inflightGets[cacheKey] = inflight
	return inflight, true
}

func (m *objectCacheStorageMiddleware) finishInflightGet(cacheKey string, inflight *inflightGet, err error) {
	if inflight == nil {
		return
	}
	m.inflightMu.Lock()
	defer m.inflightMu.Unlock()
	current, ok := m.inflightGets[cacheKey]
	if !ok || current != inflight {
		return
	}
	current.err = err
	delete(m.inflightGets, cacheKey)
	close(current.done)
}

func (m *objectCacheStorageMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	objKey := objectCacheKey(bucketName, key)
	headKey := headCacheKey(bucketName, key)
	pr, pw := io.Pipe()
	cacheWriteDone := make(chan struct{})
	go func() {
		defer close(cacheWriteDone)
		setErr := m.cache.Set(objKey, pr, -1)
		if setErr != nil {
			if !errors.Is(setErr, errObjectLargerThanCacheThreshold) {
				slog.DebugContext(ctx, "Failed to stream object body into cache on put", "key", objKey, "error", setErr)
			}
			_ = m.cache.Remove(objKey)
		}
	}()

	teedReader := &cacheOnWriteReader{
		Reader:             data,
		pipeWriter:         pw,
		maxObjectSizeBytes: m.maxObjectSizeBytes,
	}

	result, err := m.Next.PutObject(ctx, bucketName, key, contentType, teedReader, checksumInput, opts)
	if err != nil {
		_ = teedReader.closeWithError(err)
		<-cacheWriteDone
		m.invalidateObjectCaches(ctx, bucketName, key)
		return nil, err
	}
	_ = teedReader.close()
	<-cacheWriteDone

	obj, headErr := m.Next.HeadObject(ctx, bucketName, key, nil)
	if headErr == nil {
		if writeErr := m.writeHeadToCache(ctx, headKey, obj); writeErr != nil {
			slog.DebugContext(ctx, "Failed to write head cache on put", "key", headKey, "error", writeErr)
		}
	} else {
		slog.DebugContext(ctx, "Failed to head object on put for cache metadata", "key", headKey, "error", headErr)
		_ = m.cache.Remove(headKey)
	}

	return result, nil
}

type cacheOnWriteReader struct {
	io.Reader
	pipeWriter         *io.PipeWriter
	maxObjectSizeBytes int64
	bytesSeen          int64
	cachePipeActive    bool
}

func (r *cacheOnWriteReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		if !r.cachePipeActive {
			r.cachePipeActive = true
		}
		r.bytesSeen += int64(n)
		if r.bytesSeen > r.maxObjectSizeBytes {
			_ = r.pipeWriter.CloseWithError(errObjectLargerThanCacheThreshold)
			r.pipeWriter = nil
			r.cachePipeActive = false
		} else if r.pipeWriter != nil {
			if _, writeErr := r.pipeWriter.Write(p[:n]); writeErr != nil {
				_ = r.pipeWriter.CloseWithError(writeErr)
				r.pipeWriter = nil
				r.cachePipeActive = false
			}
		}
	}

	if err == io.EOF {
		if r.pipeWriter != nil {
			_ = r.pipeWriter.Close()
			r.pipeWriter = nil
			r.cachePipeActive = false
		}
	} else if err != nil {
		if r.pipeWriter != nil {
			_ = r.pipeWriter.CloseWithError(err)
			r.pipeWriter = nil
			r.cachePipeActive = false
		}
	}
	return n, err
}

func (r *cacheOnWriteReader) close() error {
	if r.pipeWriter != nil {
		_ = r.pipeWriter.Close()
		r.pipeWriter = nil
		r.cachePipeActive = false
	}
	return nil
}

func (r *cacheOnWriteReader) closeWithError(err error) error {
	if r.pipeWriter != nil {
		_ = r.pipeWriter.CloseWithError(err)
		r.pipeWriter = nil
		r.cachePipeActive = false
	}
	return nil
}

func (m *objectCacheStorageMiddleware) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	result, err := m.Next.AppendObject(ctx, bucketName, key, data, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	m.invalidateObjectCaches(ctx, bucketName, key)
	return result, nil
}

func (m *objectCacheStorageMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) error {
	err := m.Next.DeleteObject(ctx, bucketName, key, opts)
	if err != nil {
		return err
	}
	m.invalidateObjectCaches(ctx, bucketName, key)
	return nil
}

func (m *objectCacheStorageMiddleware) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
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

func (m *objectCacheStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	result, err := m.Next.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	m.invalidateObjectCaches(ctx, bucketName, key)
	return result, nil
}

func (m *objectCacheStorageMiddleware) invalidateObjectCaches(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) {
	objKey := objectCacheKey(bucketName, key)
	if err := m.cache.Remove(objKey); err != nil {
		slog.DebugContext(ctx, "Failed to remove object cache key", "key", objKey, "error", err)
	}
	headKey := headCacheKey(bucketName, key)
	if err := m.cache.Remove(headKey); err != nil {
		slog.DebugContext(ctx, "Failed to remove head cache key", "key", headKey, "error", err)
	}
}

func (m *objectCacheStorageMiddleware) readHeadFromCache(ctx context.Context, key string) (*storage.Object, error) {
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

func (m *objectCacheStorageMiddleware) readObjectFromCache(ctx context.Context, key string) (*storage.Object, error) {
	headKey := strings.Replace(key, "OBJECTCACHE_OBJECT_BODY_", "OBJECTCACHE_HEAD_", 1)
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

func (m *objectCacheStorageMiddleware) writeHeadToCache(ctx context.Context, key string, obj *storage.Object) error {
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
