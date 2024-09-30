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
