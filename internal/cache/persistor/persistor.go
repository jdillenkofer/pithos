package persistor

import (
	"errors"
	"io"
)

var (
	ErrCacheMiss = errors.New("cache miss")
)

type CachePersistor interface {
	Store(key string, reader io.Reader) (int64, error)
	Get(key string) (io.ReadCloser, error)
	Remove(key string) error
	RemoveAll() error
}
