package cache

import "errors"

var (
	ErrKeyNotInCache = errors.New("key not in cache")
)

type Cache interface {
	PutKey(key string, data []byte) error
	GetKey(key string) ([]byte, error)
	DeleteKey(key string) error
}
