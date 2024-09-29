package cache

import (
	"container/heap"
	"time"
)

type EvictionChecker interface {
	ShouldEvict() bool
	TrackSet(key string, val []byte)
	TrackRemove(key string)
}

type FixedKeyLimitEvictionChecker struct {
	maxKeyLimit int
	keySet      map[string]struct{}
}

func NewFixedKeyLimitEvictionChecker(maxKeyLimit int) (*FixedKeyLimitEvictionChecker, error) {
	return &FixedKeyLimitEvictionChecker{
		maxKeyLimit: maxKeyLimit,
		keySet:      make(map[string]struct{}),
	}, nil
}

func (f *FixedKeyLimitEvictionChecker) ShouldEvict() bool {
	return len(f.keySet) > f.maxKeyLimit
}

func (f *FixedKeyLimitEvictionChecker) TrackSet(key string, val []byte) {
	_, ok := f.keySet[key]
	if ok {
		return
	} else {
		f.keySet[key] = struct{}{}
	}
}

func (f *FixedKeyLimitEvictionChecker) TrackRemove(key string) {
	delete(f.keySet, key)
}

type FixedSizeLimitEvictionChecker struct {
	currentSize  int64
	maxSizeLimit int64
	keySet       map[string]int64
}

func NewFixedSizeLimitEvictionChecker(maxSizeLimit int64) (*FixedSizeLimitEvictionChecker, error) {
	return &FixedSizeLimitEvictionChecker{
		currentSize:  0,
		maxSizeLimit: maxSizeLimit,
		keySet:       make(map[string]int64),
	}, nil
}

func (f *FixedSizeLimitEvictionChecker) ShouldEvict() bool {
	return f.currentSize > f.maxSizeLimit
}

func (f *FixedSizeLimitEvictionChecker) TrackSet(key string, val []byte) {
	newSize := int64(len(val))
	oldSize, ok := f.keySet[key]
	if ok {
		f.currentSize -= oldSize
	}
	f.keySet[key] = newSize
	f.currentSize += newSize
}

func (f *FixedSizeLimitEvictionChecker) TrackRemove(key string) {
	oldSize, ok := f.keySet[key]
	if ok {
		f.currentSize -= oldSize
	}
	delete(f.keySet, key)
}

type CacheEvictionPolicy interface {
	TrackSetAndReturnEvictedKeys(key string, val []byte) []string
	TrackGet(key string)
	TrackRemove(key string)
}

type EvictNothingPolicy struct {
}

func (*EvictNothingPolicy) TrackSetAndReturnEvictedKeys(key string, val []byte) []string {
	return []string{}
}

func (*EvictNothingPolicy) TrackGet(key string) {

}

func (*EvictNothingPolicy) TrackRemove(key string) {

}

type LFUCacheEntry struct {
	key          string
	frequency    int
	lastAccessTs time.Time
	index        int
}

type MinLFUCacheHeap []*LFUCacheEntry

func (mh MinLFUCacheHeap) Len() int { return len(mh) }

func (mh MinLFUCacheHeap) Less(i, j int) bool {
	return mh[i].frequency < mh[j].frequency ||
		mh[i].frequency == mh[j].frequency && mh[i].lastAccessTs.Before(mh[j].lastAccessTs)
}

func (mh MinLFUCacheHeap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
	mh[i].index = i
	mh[j].index = j
}

func (mh *MinLFUCacheHeap) Push(x any) {
	n := len(*mh)
	item := x.(*LFUCacheEntry)
	item.index = n
	*mh = append(*mh, item)
}

func (mh *MinLFUCacheHeap) Pop() any {
	old := *mh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	*mh = old[0 : n-1]
	return item
}

func (mh *MinLFUCacheHeap) update(entry *LFUCacheEntry, key string, frequency int, lastAccessTs time.Time) {
	entry.key = key
	entry.frequency = frequency
	entry.lastAccessTs = lastAccessTs
	heap.Fix(mh, entry.index)
}

type LFUCacheEvictionPolicy struct {
	evictionChecker EvictionChecker
	minLFUCacheHeap MinLFUCacheHeap
}

func NewLFUCacheEvictionPolicy(evictionChecker EvictionChecker) (*LFUCacheEvictionPolicy, error) {
	return &LFUCacheEvictionPolicy{
		evictionChecker: evictionChecker,
		minLFUCacheHeap: MinLFUCacheHeap{},
	}, nil
}

func (lfu *LFUCacheEvictionPolicy) TrackSetAndReturnEvictedKeys(key string, val []byte) []string {
	lfu.evictionChecker.TrackSet(key, val)
	lfu.minLFUCacheHeap.Push(&LFUCacheEntry{
		key:          key,
		frequency:    0,
		lastAccessTs: time.Now(),
		index:        -1,
	})

	evictedKeys := []string{}
	for lfu.evictionChecker.ShouldEvict() {
		keyToEvict := lfu.minLFUCacheHeap.Pop().(string)
		lfu.evictionChecker.TrackRemove(keyToEvict)
		evictedKeys = append(evictedKeys, keyToEvict)
	}
	return evictedKeys
}

func (lfu *LFUCacheEvictionPolicy) TrackGet(key string) {
	for _, entry := range lfu.minLFUCacheHeap {
		if entry.key == key {
			lfu.minLFUCacheHeap.update(entry, key, entry.frequency+1, time.Now())
			break
		}
	}
}

func (lfu *LFUCacheEvictionPolicy) TrackRemove(key string) {
	lfu.evictionChecker.TrackRemove(key)
	for index, entry := range lfu.minLFUCacheHeap {
		if entry.key == key {
			heap.Remove(&lfu.minLFUCacheHeap, index)
			break
		}
	}
}
