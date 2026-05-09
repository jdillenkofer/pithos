package cache

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"testing"

	cachepkg "github.com/jdillenkofer/pithos/internal/storage/cache"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/stretchr/testify/assert"
)

type memoryCache struct {
	data map[string][]byte
}

func newMemoryCache() *memoryCache {
	return &memoryCache{data: map[string][]byte{}}
}

func (c *memoryCache) Set(key string, data []byte) error {
	cloned := make([]byte, len(data))
	copy(cloned, data)
	c.data[key] = cloned
	return nil
}

func (c *memoryCache) Get(key string) ([]byte, error) {
	data, ok := c.data[key]
	if !ok {
		return nil, cachepkg.ErrCacheMiss
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned, nil
}

func (c *memoryCache) Remove(key string) error {
	delete(c.data, key)
	return nil
}

type memoryPartStore struct {
	parts       map[partstore.PartId][]byte
	getPartCall int
}

func newMemoryPartStore() *memoryPartStore {
	return &memoryPartStore{parts: map[partstore.PartId][]byte{}}
}

func (s *memoryPartStore) Start(ctx context.Context) error { return nil }
func (s *memoryPartStore) Stop(ctx context.Context) error  { return nil }

func (s *memoryPartStore) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.parts[partId] = data
	return nil
}

func (s *memoryPartStore) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	s.getPartCall++
	data, ok := s.parts[partId]
	if !ok {
		return nil, partstore.ErrPartNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *memoryPartStore) GetPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	ids := make([]partstore.PartId, 0, len(s.parts))
	for id := range s.parts {
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *memoryPartStore) DeletePart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	delete(s.parts, partId)
	return nil
}

func TestCachePartStore_GetPartCachesAndHits(t *testing.T) {
	ctx := context.Background()
	cache := newMemoryCache()
	inner := newMemoryPartStore()
	partId, _ := partstore.NewRandomPartId()
	inner.parts[*partId] = []byte("hello")

	store, err := New(cache, inner, Options{MaxPartSizeBytes: 1024, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	rc, err := store.GetPart(ctx, nil, *partId)
	assert.NoError(t, err)
	data, err := io.ReadAll(rc)
	assert.NoError(t, err)
	assert.NoError(t, rc.Close())
	assert.Equal(t, []byte("hello"), data)

	rc, err = store.GetPart(ctx, nil, *partId)
	assert.NoError(t, err)
	data, err = io.ReadAll(rc)
	assert.NoError(t, err)
	assert.NoError(t, rc.Close())
	assert.Equal(t, []byte("hello"), data)
	assert.Equal(t, 1, inner.getPartCall)
}

func TestCachePartStore_PutAndDeleteInvalidate(t *testing.T) {
	ctx := context.Background()
	cache := newMemoryCache()
	inner := newMemoryPartStore()
	partId, _ := partstore.NewRandomPartId()

	store, err := New(cache, inner, Options{MaxPartSizeBytes: 1024, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	err = store.PutPart(ctx, nil, *partId, bytes.NewReader([]byte("v1")))
	assert.NoError(t, err)

	rc, err := store.GetPart(ctx, nil, *partId)
	assert.NoError(t, err)
	data, err := io.ReadAll(rc)
	assert.NoError(t, err)
	assert.NoError(t, rc.Close())
	assert.Equal(t, []byte("v1"), data)

	err = store.DeletePart(ctx, nil, *partId)
	assert.NoError(t, err)

	_, err = store.GetPart(ctx, nil, *partId)
	assert.ErrorIs(t, err, partstore.ErrPartNotFound)
}

func TestCachePartStore_MaxSizeBypassesCache(t *testing.T) {
	ctx := context.Background()
	cache := newMemoryCache()
	inner := newMemoryPartStore()
	partId, _ := partstore.NewRandomPartId()
	inner.parts[*partId] = []byte("abcdef")

	store, err := New(cache, inner, Options{MaxPartSizeBytes: 2, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	rc, err := store.GetPart(ctx, nil, *partId)
	assert.NoError(t, err)
	_, err = io.ReadAll(rc)
	assert.NoError(t, err)
	assert.NoError(t, rc.Close())

	rc, err = store.GetPart(ctx, nil, *partId)
	assert.NoError(t, err)
	_, err = io.ReadAll(rc)
	assert.NoError(t, err)
	assert.NoError(t, rc.Close())

	assert.Equal(t, 2, inner.getPartCall)
}
