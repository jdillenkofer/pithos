package persistor

import (
	"errors"
)

var (
	ErrCacheMiss = errors.New("cache miss")
)

type CachePersistor interface {
	Store(key string, val []byte) error
	Get(key string) ([]byte, error)
	Remove(key string) error
	RemoveAll() error
}
