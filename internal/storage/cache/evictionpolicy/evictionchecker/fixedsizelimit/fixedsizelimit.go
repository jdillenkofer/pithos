package fixedsizelimit

type fixedSizeLimitEvictionChecker struct {
	currentSize  int64
	maxSizeLimit int64
	keySet       map[string]int64
}

func New(maxSizeLimit int64) (*fixedSizeLimitEvictionChecker, error) {
	return &fixedSizeLimitEvictionChecker{
		currentSize:  0,
		maxSizeLimit: maxSizeLimit,
		keySet:       make(map[string]int64),
	}, nil
}

func (f *fixedSizeLimitEvictionChecker) ShouldEvict() bool {
	return f.currentSize > f.maxSizeLimit
}

func (f *fixedSizeLimitEvictionChecker) TrackSet(key string, val []byte) {
	newSize := int64(len(val))
	oldSize, ok := f.keySet[key]
	if ok {
		f.currentSize -= oldSize
	}
	f.keySet[key] = newSize
	f.currentSize += newSize
}

func (f *fixedSizeLimitEvictionChecker) TrackRemove(key string) {
	oldSize, ok := f.keySet[key]
	if ok {
		f.currentSize -= oldSize
	}
	delete(f.keySet, key)
}
