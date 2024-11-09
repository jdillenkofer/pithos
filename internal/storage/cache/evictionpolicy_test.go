package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFixedKeyLimitEvictionChecker(t *testing.T) {
	ec, err := NewFixedKeyLimitEvictionChecker(3)
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
	ec, err := NewFixedSizeLimitEvictionChecker(3)
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
	ec, err := NewFixedSizeLimitEvictionChecker(3)
	assert.Nil(t, err)

	lfuCacheEvictionPolicy, err := NewLFUCacheEvictionPolicy(ec)
	assert.Nil(t, err)

	assert.Empty(t, lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("a", []byte{0x0}))
	assert.Empty(t, lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("b", []byte{0x1}))
	assert.Empty(t, lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("c", []byte{0x2}))
	lfuCacheEvictionPolicy.TrackGet("a")
	lfuCacheEvictionPolicy.TrackGet("b")
	lfuCacheEvictionPolicy.TrackGet("c")

	evictedKeys := lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("d", []byte{0x0})
	assert.Equal(t, 1, len(evictedKeys))
	assert.Equal(t, "a", evictedKeys[0])
	lfuCacheEvictionPolicy.TrackGet("d")

	evictedKeys2 := lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("e", []byte{0x0})
	assert.Equal(t, 1, len(evictedKeys2))
	assert.Equal(t, "b", evictedKeys2[0])
	lfuCacheEvictionPolicy.TrackGet("e")

	evictedKeys3 := lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("f", []byte{0x0})
	assert.Equal(t, 1, len(evictedKeys3))
	assert.Equal(t, "c", evictedKeys3[0])

	evictedKeys4 := lfuCacheEvictionPolicy.TrackSetAndReturnEvictedKeys("g", []byte{0x0})
	assert.Equal(t, 1, len(evictedKeys4))
	assert.Equal(t, "f", evictedKeys4[0])
}
