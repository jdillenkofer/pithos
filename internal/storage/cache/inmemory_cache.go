package cache

import "sync"

type InMemoryCache struct {
	mu            sync.Mutex
	keyToValueMap map[string][]byte
}

func NewInMemoryCache() (*InMemoryCache, error) {
	return &InMemoryCache{
		mu:            sync.Mutex{},
		keyToValueMap: make(map[string][]byte),
	}, nil
}

func (c *InMemoryCache) PutKey(key string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keyToValueMap[key] = data
	return nil
}

func (c *InMemoryCache) GetKey(key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.keyToValueMap[key]
	if !ok {
		return nil, ErrKeyNotInCache
	}
	return data, nil
}

func (c *InMemoryCache) DeleteKey(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.keyToValueMap, key)
	return nil
}
