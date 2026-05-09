package inmemory

import (
	"bytes"
	"io"

	"github.com/jdillenkofer/pithos/internal/cache/persistor"
)

type inMemoryCachePersistor struct {
	keyToCacheEntryMap map[string][]byte
}

func New() (persistor.CachePersistor, error) {
	return &inMemoryCachePersistor{
		keyToCacheEntryMap: make(map[string][]byte),
	}, nil
}

func (cs *inMemoryCachePersistor) Store(key string, reader io.Reader) error {
	val, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	cs.keyToCacheEntryMap[key] = val
	return nil
}

func (cs *inMemoryCachePersistor) Get(key string) (io.ReadCloser, error) {
	val, ok := cs.keyToCacheEntryMap[key]
	if !ok {
		return nil, persistor.ErrCacheMiss
	}
	return io.NopCloser(bytes.NewReader(val)), nil
}

func (cs *inMemoryCachePersistor) Remove(key string) error {
	delete(cs.keyToCacheEntryMap, key)
	return nil
}

func (cs *inMemoryCachePersistor) RemoveAll() error {
	for key := range cs.keyToCacheEntryMap {
		delete(cs.keyToCacheEntryMap, key)
	}
	return nil
}
