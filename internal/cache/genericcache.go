package cache

import (
	"io"
	"sync"

	"github.com/jdillenkofer/pithos/internal/cache/evictionpolicy"
	"github.com/jdillenkofer/pithos/internal/cache/persistor"
)

type GenericCache struct {
	mu                  sync.Mutex
	cachePersistor      persistor.CachePersistor
	cacheEvictionPolicy evictionpolicy.CacheEvictionPolicy
}

func NewGenericCache(cachePersistor persistor.CachePersistor, cacheEvictionPolicy evictionpolicy.CacheEvictionPolicy) (*GenericCache, error) {
	return &GenericCache{
		mu:                  sync.Mutex{},
		cachePersistor:      cachePersistor,
		cacheEvictionPolicy: cacheEvictionPolicy,
	}, nil
}

func (c *GenericCache) Set(key string, reader io.Reader, size int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	evictedKeys := c.cacheEvictionPolicy.TrackSetAndReturnEvictedKeys(key, size)
	for _, evictedKey := range evictedKeys {
		err := c.cachePersistor.Remove(evictedKey)
		if err != nil {
			return err
		}
	}

	err := c.cachePersistor.Store(key, reader)
	if err != nil {
		return err
	}
	return nil
}

func (c *GenericCache) Get(key string) (io.ReadCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cacheEvictionPolicy.TrackGet(key)

	val, err := c.cachePersistor.Get(key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (c *GenericCache) Remove(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cacheEvictionPolicy.TrackRemove(key)

	err := c.cachePersistor.Remove(key)
	if err != nil {
		return err
	}
	return nil
}
