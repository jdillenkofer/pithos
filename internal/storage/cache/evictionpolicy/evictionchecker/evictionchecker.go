package evictionchecker

type EvictionChecker interface {
	ShouldEvict() bool
	TrackSet(key string, val []byte)
	TrackRemove(key string)
}
