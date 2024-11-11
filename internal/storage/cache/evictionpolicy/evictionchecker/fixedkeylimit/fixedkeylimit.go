package fixedkeylimit

type fixedKeyLimitEvictionChecker struct {
	maxKeyLimit int
	keySet      map[string]struct{}
}

func New(maxKeyLimit int) (*fixedKeyLimitEvictionChecker, error) {
	return &fixedKeyLimitEvictionChecker{
		maxKeyLimit: maxKeyLimit,
		keySet:      make(map[string]struct{}),
	}, nil
}

func (f *fixedKeyLimitEvictionChecker) ShouldEvict() bool {
	return len(f.keySet) > f.maxKeyLimit
}

func (f *fixedKeyLimitEvictionChecker) TrackSet(key string, val []byte) {
	_, ok := f.keySet[key]
	if ok {
		return
	} else {
		f.keySet[key] = struct{}{}
	}
}

func (f *fixedKeyLimitEvictionChecker) TrackRemove(key string) {
	delete(f.keySet, key)
}
