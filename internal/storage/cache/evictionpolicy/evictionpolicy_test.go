package evictionpolicy_test

import (
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker/fixedkeylimit"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker/fixedsizelimit"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/lfu"
	"github.com/stretchr/testify/assert"
)

func TestFixedKeyLimitEvictionChecker(t *testing.T) {
	ec, err := fixedkeylimit.New(3)
	assert.Nil(t, err)

	assert.False(t, ec.ShouldEvict())
	ec.TrackSet("a", []byte{})
	assert.False(t, ec.ShouldEvict())
	ec.TrackSet("b", []byte{})
	assert.False(t, ec.ShouldEvict())
	ec.TrackSet("c", []byte{})
	assert.False(t, ec.ShouldEvict())
	ec.TrackSet("d", []byte{})
	assert.True(t, ec.ShouldEvict())
	ec.TrackRemove("a")
	assert.False(t, ec.ShouldEvict())
}

func TestFixedSizeLimitEvictionChecker(t *testing.T) {
	ec, err := fixedsizelimit.New(3)
	assert.Nil(t, err)

	assert.False(t, ec.ShouldEvict())
	ec.TrackSet("a", []byte{0, 1})
	assert.False(t, ec.ShouldEvict())
	ec.TrackSet("b", []byte{2})
	assert.False(t, ec.ShouldEvict())
	ec.TrackSet("c", []byte{3})
	assert.True(t, ec.ShouldEvict())
	ec.TrackRemove("a")
	assert.False(t, ec.ShouldEvict())
}

func TestLFUCacheEvictionPolicy(t *testing.T) {
	ec, err := fixedsizelimit.New(3)
	assert.Nil(t, err)

	// The lfuCacheEvictionPolicy is sensitive to timing.
	// The time package uses a monotonic clock,
	// but it is allowed to return a value equal to the time,
	// we read in the last time.Now() call.
	lfuCacheEvictionPolicy, err := lfu.New(ec)
	assert.Nil(t, err)

	assert.Empty(t, lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("a", []byte{0x0}))
	time.Sleep(200 * time.Millisecond)
	assert.Empty(t, lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("b", []byte{0x1}))
	time.Sleep(200 * time.Millisecond)
	assert.Empty(t, lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("c", []byte{0x2}))
	time.Sleep(200 * time.Millisecond)
	lfuCacheEvictionPolicy.TrackGet("a")
	time.Sleep(200 * time.Millisecond)
	lfuCacheEvictionPolicy.TrackGet("b")
	time.Sleep(200 * time.Millisecond)
	lfuCacheEvictionPolicy.TrackGet("c")

	time.Sleep(200 * time.Millisecond)
	evictedKeys := lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("d", []byte{0x0})
	assert.Equal(t, 1, len(evictedKeys))
	assert.Equal(t, "a", evictedKeys[0])
	lfuCacheEvictionPolicy.TrackGet("d")

	time.Sleep(200 * time.Millisecond)
	evictedKeys2 := lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("e", []byte{0x0})
	assert.Equal(t, 1, len(evictedKeys2))
	assert.Equal(t, "b", evictedKeys2[0])
	lfuCacheEvictionPolicy.TrackGet("e")

	time.Sleep(200 * time.Millisecond)
	evictedKeys3 := lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("f", []byte{0x0})
	assert.Equal(t, 1, len(evictedKeys3))
	assert.Equal(t, "c", evictedKeys3[0])

	time.Sleep(200 * time.Millisecond)
	evictedKeys4 := lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("g", []byte{0x0})
	assert.Equal(t, 1, len(evictedKeys4))
	assert.Equal(t, "f", evictedKeys4[0])
}
