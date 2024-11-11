package lfu

import (
	"container/heap"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker"
)

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
	evictionChecker evictionchecker.EvictionChecker
	minLFUCacheHeap MinLFUCacheHeap
}

func New(evictionChecker evictionchecker.EvictionChecker) (evictionpolicy.CacheEvictionPolicy, error) {
	return &LFUCacheEvictionPolicy{
		evictionChecker: evictionChecker,
		minLFUCacheHeap: MinLFUCacheHeap{},
	}, nil
}

func (lfu *LFUCacheEvictionPolicy) TrackSetAndReturnEvictedKeys(key string, val []byte) []string {
	lfu.evictionChecker.TrackSet(key, val)

	evictedKeys := []string{}
	for lfu.evictionChecker.ShouldEvict() {
		cacheEntryToEvict := heap.Pop(&lfu.minLFUCacheHeap).(*LFUCacheEntry)
		lfu.evictionChecker.TrackRemove(cacheEntryToEvict.key)
		evictedKeys = append(evictedKeys, cacheEntryToEvict.key)
	}

	heap.Push(&lfu.minLFUCacheHeap, &LFUCacheEntry{
		key:          key,
		frequency:    0,
		lastAccessTs: time.Now(),
		index:        -1,
	})
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
