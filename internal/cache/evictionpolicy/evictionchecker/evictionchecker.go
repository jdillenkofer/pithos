package evictionchecker

type EvictionChecker interface {
	ShouldEvict() bool
	TrackSet(key string, size int64)
	TrackRemove(key string)
}
