package evictnothing

import "github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy"

type EvictNothingPolicy struct {
}

func New() (evictionpolicy.CacheEvictionPolicy, error) {
	return &EvictNothingPolicy{}, nil
}

func (*EvictNothingPolicy) TrackSetAndReturnEvictedKeys(key string, val []byte) []string {
	return []string{}
}

func (*EvictNothingPolicy) TrackGet(key string) {

}

func (*EvictNothingPolicy) TrackRemove(key string) {

}
