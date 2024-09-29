package cache

import "sync"

type cacheEntry struct {
	key string
	val []byte
}

type InMemoryCache struct {
	mu                 sync.Mutex
	keyToCacheEntryMap map[string]cacheEntry
}

func NewInMemoryCache() (*InMemoryCache, error) {
	return &InMemoryCache{
		mu:                 sync.Mutex{},
		keyToCacheEntryMap: make(map[string]cacheEntry),
	}, nil
}

func (c *InMemoryCache) Put(key string, val []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := cacheEntry{
		key: key,
		val: val,
	}
	c.keyToCacheEntryMap[key] = entry
	return nil
}

func (c *InMemoryCache) Get(key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.keyToCacheEntryMap[key]
	if !ok {
		return nil, ErrCacheMiss
	}
	return entry.val, nil
}

func (c *InMemoryCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.keyToCacheEntryMap, key)
	return nil
}
