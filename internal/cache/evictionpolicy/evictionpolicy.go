package evictionpolicy

type CacheEvictionPolicy interface {
	TrackSetAndReturnEvictedKeys(key string, val []byte) []string
	TrackGet(key string)
	TrackRemove(key string)
}
