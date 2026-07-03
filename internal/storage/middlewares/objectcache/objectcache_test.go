package objectcache

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	cachepkg "github.com/jdillenkofer/pithos/internal/cache"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/stretchr/testify/assert"
)

type memoryCache struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemoryCache() *memoryCache {
	return &memoryCache{data: map[string][]byte{}}
}

func (c *memoryCache) Set(key string, reader io.Reader, size int64) error {
	_ = size
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	c.mu.Lock()
	c.data[key] = buf
	c.mu.Unlock()
	return nil
}

func (c *memoryCache) Get(key string) (io.ReadCloser, error) {
	c.mu.Lock()
	data, ok := c.data[key]
	c.mu.Unlock()
	if !ok {
		return nil, cachepkg.ErrCacheMiss
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	return io.NopCloser(bytes.NewReader(buf)), nil
}

func (c *memoryCache) Remove(key string) error {
	c.mu.Lock()
	delete(c.data, key)
	c.mu.Unlock()
	return nil
}

type fakeStorage struct {
	storage.Storage
	mu          sync.Mutex
	bodyByKey   map[string][]byte
	objectByKey map[string]storage.Object
	headCalls   map[string]int
	getCalls    map[string]int
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{
		bodyByKey:   map[string][]byte{},
		objectByKey: map[string]storage.Object{},
		headCalls:   map[string]int{},
		getCalls:    map[string]int{},
	}
}

func (s *fakeStorage) Start(ctx context.Context) error { return nil }
func (s *fakeStorage) Stop(ctx context.Context) error  { return nil }

func (s *fakeStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	k := bucketName.String() + "/" + key.String()
	s.mu.Lock()
	s.headCalls[k]++
	obj, ok := s.objectByKey[k]
	s.mu.Unlock()
	if !ok {
		return nil, storage.ErrNoSuchKey
	}
	if opts != nil && opts.IfMatchETag != nil && *opts.IfMatchETag != storage.ETagWildcard && obj.ETag != *opts.IfMatchETag {
		return nil, storage.ErrPreconditionFailed
	}
	if opts != nil && opts.IfNoneMatchETag != nil {
		if *opts.IfNoneMatchETag == storage.ETagWildcard || obj.ETag == *opts.IfNoneMatchETag {
			return nil, storage.ErrNotModified
		}
	}
	clone := obj
	return &clone, nil
}

func (s *fakeStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	k := bucketName.String() + "/" + key.String()
	s.mu.Lock()
	s.getCalls[k]++
	obj, ok := s.objectByKey[k]
	body := s.bodyByKey[k]
	s.mu.Unlock()
	if !ok {
		return nil, nil, storage.ErrNoSuchKey
	}
	if opts != nil && opts.IfMatchETag != nil && *opts.IfMatchETag != storage.ETagWildcard && obj.ETag != *opts.IfMatchETag {
		return nil, nil, storage.ErrPreconditionFailed
	}
	if opts != nil && opts.IfNoneMatchETag != nil {
		if *opts.IfNoneMatchETag == storage.ETagWildcard || obj.ETag == *opts.IfNoneMatchETag {
			return nil, nil, storage.ErrNotModified
		}
	}
	return &obj, []io.ReadCloser{io.NopCloser(bytes.NewReader(body))}, nil
}

func (s *fakeStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	body, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	k := bucketName.String() + "/" + key.String()
	etag := "etag-" + time.Now().String()
	s.mu.Lock()
	s.bodyByKey[k] = body
	s.objectByKey[k] = storage.Object{Key: key, ETag: etag, Size: int64(len(body))}
	s.mu.Unlock()
	return &storage.PutObjectResult{ETag: &etag}, nil
}

func (s *fakeStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	k := bucketName.String() + "/" + key.String()
	s.mu.Lock()
	delete(s.bodyByKey, k)
	delete(s.objectByKey, k)
	s.mu.Unlock()
	return &storage.DeleteObjectResult{}, nil
}

func TestReadCacheMiddleware_CachesGetObject(t *testing.T) {
	ctx := context.Background()
	bucket := metadatastore.MustNewBucketName("bucket")
	key := metadatastore.MustNewObjectKey("key")

	inner := newFakeStorage()
	inner.objectByKey["bucket/key"] = storage.Object{Key: key, ETag: "e1", Size: 5}
	inner.bodyByKey["bucket/key"] = []byte("hello")

	mw, err := NewStorageMiddleware(inner, newMemoryCache(), Options{MaxObjectSizeBytes: 1024, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	obj, readers, err := mw.GetObject(ctx, bucket, key, nil, nil)
	assert.NoError(t, err)
	body, err := io.ReadAll(readers[0])
	assert.NoError(t, err)
	assert.NoError(t, readers[0].Close())
	assert.Equal(t, int64(5), obj.Size)
	assert.Equal(t, []byte("hello"), body)

	obj, readers, err = mw.GetObject(ctx, bucket, key, nil, nil)
	assert.NoError(t, err)
	body, err = io.ReadAll(readers[0])
	assert.NoError(t, err)
	assert.NoError(t, readers[0].Close())
	assert.Equal(t, []byte("hello"), body)
	assert.Equal(t, 1, inner.getCalls["bucket/key"])
}

func TestReadCacheMiddleware_InvalidatesOnPutDelete(t *testing.T) {
	ctx := context.Background()
	bucket := metadatastore.MustNewBucketName("bucket")
	key := metadatastore.MustNewObjectKey("key")

	inner := newFakeStorage()
	inner.objectByKey["bucket/key"] = storage.Object{Key: key, ETag: "e1", Size: 5}
	inner.bodyByKey["bucket/key"] = []byte("hello")

	mw, err := NewStorageMiddleware(inner, newMemoryCache(), Options{MaxObjectSizeBytes: 1024, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	_, readers, err := mw.GetObject(ctx, bucket, key, nil, nil)
	assert.NoError(t, err)
	_, _ = io.ReadAll(readers[0])
	_ = readers[0].Close()

	_, err = mw.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("world")), nil, nil)
	assert.NoError(t, err)

	_, readers, err = mw.GetObject(ctx, bucket, key, nil, nil)
	assert.NoError(t, err)
	body, err := io.ReadAll(readers[0])
	assert.NoError(t, err)
	assert.NoError(t, readers[0].Close())
	assert.Equal(t, []byte("world"), body)

	_, err = mw.DeleteObject(ctx, bucket, key, nil)
	assert.NoError(t, err)
	_, _, err = mw.GetObject(ctx, bucket, key, nil, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
}

func TestReadCacheMiddleware_CachesBodyOnPutObject(t *testing.T) {
	ctx := context.Background()
	bucket := metadatastore.MustNewBucketName("bucket")
	key := metadatastore.MustNewObjectKey("key")

	inner := newFakeStorage()
	mw, err := NewStorageMiddleware(inner, newMemoryCache(), Options{MaxObjectSizeBytes: 1024, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	_, err = mw.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	assert.NoError(t, err)

	_, readers, err := mw.GetObject(ctx, bucket, key, nil, nil)
	assert.NoError(t, err)
	body, err := io.ReadAll(readers[0])
	assert.NoError(t, err)
	assert.NoError(t, readers[0].Close())
	assert.Equal(t, []byte("hello"), body)
	assert.Equal(t, 0, inner.getCalls["bucket/key"])
}

func TestReadCacheMiddleware_CoalescesConcurrentGetMisses(t *testing.T) {
	ctx := context.Background()
	bucket := metadatastore.MustNewBucketName("bucket")
	key := metadatastore.MustNewObjectKey("key")

	inner := newFakeStorage()
	inner.objectByKey["bucket/key"] = storage.Object{Key: key, ETag: "e1", Size: 5}
	inner.bodyByKey["bucket/key"] = []byte("hello")

	mw, err := NewStorageMiddleware(inner, newMemoryCache(), Options{MaxObjectSizeBytes: 1024, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, readers, getErr := mw.GetObject(ctx, bucket, key, nil, nil)
			assert.NoError(t, getErr)
			body, readErr := io.ReadAll(readers[0])
			assert.NoError(t, readErr)
			assert.NoError(t, readers[0].Close())
			assert.Equal(t, []byte("hello"), body)
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, 1, inner.getCalls["bucket/key"])
}
