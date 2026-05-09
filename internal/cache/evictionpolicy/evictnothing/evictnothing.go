package evictnothing

import "github.com/jdillenkofer/pithos/internal/cache/evictionpolicy"

type EvictNothingPolicy struct {
}

func New() (evictionpolicy.CacheEvictionPolicy, error) {
	return &EvictNothingPolicy{}, nil
}

func (*EvictNothingPolicy) TrackSetAndReturnEvictedKeys(key string, size int64) []string {
	_ = key
	_ = size
	return []string{}
}

func (*EvictNothingPolicy) TrackGet(key string) {

}

func (*EvictNothingPolicy) TrackRemove(key string) {

}
