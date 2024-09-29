package cache

import "errors"

var (
	ErrCacheMiss = errors.New("cache miss")
)

type Cache interface {
	Put(key string, data []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}
