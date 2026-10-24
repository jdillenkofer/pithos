package partstore

import (
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTemporaryPartFilename(t *testing.T) {
	testutils.SkipIfIntegration(t)
	id, err := NewRandomPartId()
	require.NoError(t, err)
	first := NewTemporaryPartFilename(*id)
	second := NewTemporaryPartFilename(*id)
	assert.True(t, IsTemporaryPartFilename(first))
	assert.True(t, IsTemporaryPartFilename(second))
	assert.NotEqual(t, first, second)
}

func TestIsTemporaryPartFilename(t *testing.T) {
	testutils.SkipIfIntegration(t)
	const id = "0123456789abcdef0123456789abcdef"
	for _, name := range []string{
		"." + id + ".tmp.01ARZ3NDEKTSV4RRFFQ69G5FAV",
	} {
		assert.True(t, IsTemporaryPartFilename(name), name)
	}
	for _, name := range []string{
		"", id, id + ".txbackup.01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"." + id + ".123456.tmp", "." + id + "..tmp", "." + id + ".unknown.tmp",
		"." + id + ".tmp.invalid", "." + id + ".123.tmp.bak",
		".zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz.123.tmp",
	} {
		assert.False(t, IsTemporaryPartFilename(name), name)
	}
}
