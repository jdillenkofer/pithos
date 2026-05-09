package cache

import (
	"io"

	"github.com/jdillenkofer/pithos/internal/cache/persistor"
)

var (
	ErrCacheMiss = persistor.ErrCacheMiss
)

type Cache interface {
	Set(key string, reader io.Reader, size int64) error
	Get(key string) (io.ReadCloser, error)
	Remove(key string) error
}
