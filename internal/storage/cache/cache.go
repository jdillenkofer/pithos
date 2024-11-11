package cache

import (
	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor"
)

var (
	ErrCacheMiss = persistor.ErrCacheMiss
)

type Cache interface {
	Set(key string, data []byte) error
	Get(key string) ([]byte, error)
	Remove(key string) error
}
