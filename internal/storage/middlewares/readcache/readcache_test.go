package readcache

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	cachepkg "github.com/jdillenkofer/pithos/internal/cache"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/stretchr/testify/assert"
)

type memoryCache struct {
	data map[string][]byte
}

func newMemoryCache() *memoryCache {
	return &memoryCache{data: map[string][]byte{}}
}

func (c *memoryCache) Set(key string, data []byte) error {
	buf := make([]byte, len(data))
	copy(buf, data)
	c.data[key] = buf
	return nil
}

func (c *memoryCache) Get(key string) ([]byte, error) {
	data, ok := c.data[key]
	if !ok {
		return nil, cachepkg.ErrCacheMiss
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	return buf, nil
}

func (c *memoryCache) Remove(key string) error {
	delete(c.data, key)
	return nil
}

type fakeStorage struct {
	storage.Storage
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
	s.headCalls[k]++
	obj, ok := s.objectByKey[k]
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
	s.getCalls[k]++
	obj, ok := s.objectByKey[k]
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
	body := s.bodyByKey[k]
	return &obj, []io.ReadCloser{io.NopCloser(bytes.NewReader(body))}, nil
}

func (s *fakeStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	body, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	k := bucketName.String() + "/" + key.String()
	etag := "etag-" + time.Now().String()
	s.bodyByKey[k] = body
	s.objectByKey[k] = storage.Object{Key: key, ETag: etag, Size: int64(len(body))}
	return &storage.PutObjectResult{ETag: &etag}, nil
}

func (s *fakeStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) error {
	k := bucketName.String() + "/" + key.String()
	delete(s.bodyByKey, k)
	delete(s.objectByKey, k)
	return nil
}

func TestS3ReadCacheMiddleware_CachesGetObject(t *testing.T) {
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

func TestS3ReadCacheMiddleware_InvalidatesOnPutDelete(t *testing.T) {
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

	err = mw.DeleteObject(ctx, bucket, key, nil)
	assert.NoError(t, err)
	_, _, err = mw.GetObject(ctx, bucket, key, nil, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
}
