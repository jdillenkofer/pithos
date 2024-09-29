package cache

import "errors"

var (
	ErrCacheMiss = errors.New("cache miss")
)

type Cache interface {
	Set(key string, data []byte) error
	Get(key string) ([]byte, error)
	Remove(key string) error
}
