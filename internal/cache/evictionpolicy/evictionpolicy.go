package evictionpolicy

type CacheEvictionPolicy interface {
	TrackSetAndReturnEvictedKeys(key string, size int64) []string
	TrackGet(key string)
	TrackRemove(key string)
}
