package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	cachepkg "github.com/jdillenkofer/pithos/internal/cache"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/stretchr/testify/assert"
)

type memoryCache struct {
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
	cloned := make([]byte, len(data))
	copy(cloned, data)
	c.data[key] = cloned
	return nil
}

func readAllCacheEntry(c *memoryCache, key string) ([]byte, error) {
	rc, err := c.Get(key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (c *memoryCache) Get(key string) (io.ReadCloser, error) {
	data, ok := c.data[key]
	if !ok {
		return nil, cachepkg.ErrCacheMiss
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return io.NopCloser(bytes.NewReader(cloned)), nil
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

func (s *memoryPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.parts[partId] = data
	return nil
}

func (s *memoryPartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	s.getPartCall++
	data, ok := s.parts[partId]
	if !ok {
		return nil, partstore.ErrPartNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *memoryPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	ids := make([]partstore.PartId, 0, len(s.parts))
	for id := range s.parts {
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *memoryPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
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

func TestCachePartStore_SkipsMutatingCacheInsideTxByDefault(t *testing.T) {
	ctx := context.Background()
	cache := newMemoryCache()
	inner := newMemoryPartStore()
	partId, _ := partstore.NewRandomPartId()

	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "pithos.db"))
	assert.NoError(t, err)
	defer db.Close()

	store, err := New(cache, inner, Options{MaxPartSizeBytes: 1024, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := store.PutPart(ctx, tx, *partId, bytes.NewReader([]byte("v1"))); err != nil {
			return err
		}

		_, err := readAllCacheEntry(cache, getPartCacheKey(*partId))
		assert.ErrorIs(t, err, cachepkg.ErrCacheMiss)

		return store.DeletePart(ctx, tx, *partId)
	})
	assert.NoError(t, err)
}

func TestCachePartStore_AppliesPendingMutationsOnTxCommit(t *testing.T) {
	ctx := context.Background()
	cache := newMemoryCache()
	inner := newMemoryPartStore()
	partId, _ := partstore.NewRandomPartId()

	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "pithos.db"))
	assert.NoError(t, err)
	defer db.Close()
	store, err := New(cache, inner, Options{MaxPartSizeBytes: 1024, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := store.PutPart(ctx, tx, *partId, bytes.NewReader([]byte("v1"))); err != nil {
			return err
		}

		_, err := readAllCacheEntry(cache, getPartCacheKey(*partId))
		assert.ErrorIs(t, err, cachepkg.ErrCacheMiss)

		return nil
	})
	assert.NoError(t, err)

	data, err := readAllCacheEntry(cache, getPartCacheKey(*partId))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), data)
}

func TestCachePartStore_DropsPendingMutationsOnTxRollback(t *testing.T) {
	ctx := context.Background()
	cache := newMemoryCache()
	inner := newMemoryPartStore()
	partId, _ := partstore.NewRandomPartId()

	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "pithos.db"))
	assert.NoError(t, err)
	defer db.Close()

	store, err := New(cache, inner, Options{MaxPartSizeBytes: 1024, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	errRollback := fmt.Errorf("rollback")
	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := store.PutPart(ctx, tx, *partId, bytes.NewReader([]byte("v1"))); err != nil {
			return err
		}
		return errRollback
	})
	assert.ErrorIs(t, err, errRollback)

	_, err = readAllCacheEntry(cache, getPartCacheKey(*partId))
	assert.ErrorIs(t, err, cachepkg.ErrCacheMiss)
}

func TestCachePartStore_DoesNotCachePartialReads(t *testing.T) {
	ctx := context.Background()
	cache := newMemoryCache()
	inner := newMemoryPartStore()
	partId, _ := partstore.NewRandomPartId()
	inner.parts[*partId] = []byte("abcdef")

	store, err := New(cache, inner, Options{MaxPartSizeBytes: 2, CacheReadErrorsAsMiss: true})
	assert.NoError(t, err)

	rc, err := store.GetPart(ctx, nil, *partId)
	assert.NoError(t, err)

	buf := make([]byte, 2)
	_, err = rc.Read(buf)
	assert.NoError(t, err)
	assert.NoError(t, rc.Close())

	rc, err = store.GetPart(ctx, nil, *partId)
	assert.NoError(t, err)
	_, err = io.ReadAll(rc)
	assert.NoError(t, err)
	assert.NoError(t, rc.Close())

	assert.Equal(t, 2, inner.getPartCall)
}
