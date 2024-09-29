package cache

type CachePersistor interface {
	Store(key string, val []byte) error
	Get(key string) ([]byte, error)
	Remove(key string) error
	RemoveAll() error
}

type InMemoryCachePersistor struct {
	keyToCacheEntryMap map[string][]byte
}

func NewInMemoryCachePersistor() (*InMemoryCachePersistor, error) {
	return &InMemoryCachePersistor{
		keyToCacheEntryMap: make(map[string][]byte),
	}, nil
}

func (cs *InMemoryCachePersistor) Store(key string, val []byte) error {
	cs.keyToCacheEntryMap[key] = val
	return nil
}

func (cs *InMemoryCachePersistor) Get(key string) ([]byte, error) {
	val, ok := cs.keyToCacheEntryMap[key]
	if !ok {
		return nil, ErrCacheMiss
	}
	return val, nil
}

func (cs *InMemoryCachePersistor) Remove(key string) error {
	delete(cs.keyToCacheEntryMap, key)
	return nil
}

func (cs *InMemoryCachePersistor) RemoveAll() error {
	for key := range cs.keyToCacheEntryMap {
		delete(cs.keyToCacheEntryMap, key)
	}
	return nil
}
