package cache

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/jdillenkofer/pithos/internal/cache/evictionpolicy/evictnothing"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

type fakeCachePersistor struct {
	removeAllCalls int
	removeAllErr   error
}

func (f *fakeCachePersistor) Store(key string, reader io.Reader) (int64, error) {
	_ = key
	_, _ = io.Copy(io.Discard, reader)
	return 0, nil
}

func (f *fakeCachePersistor) Get(key string) (io.ReadCloser, error) {
	_ = key
	return io.NopCloser(bytes.NewReader(nil)), nil
}

func (f *fakeCachePersistor) Remove(key string) error {
	_ = key
	return nil
}

func (f *fakeCachePersistor) RemoveAll() error {
	f.removeAllCalls++
	return f.removeAllErr
}

func TestNewGenericCache_CallsRemoveAll(t *testing.T) {
	testutils.SkipIfIntegration(t)

	p := &fakeCachePersistor{}
	ep, err := evictnothing.New()
	assert.NoError(t, err)

	c, err := NewGenericCache(p, ep)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, 1, p.removeAllCalls)
}

func TestNewGenericCache_FailsWhenRemoveAllFails(t *testing.T) {
	testutils.SkipIfIntegration(t)

	p := &fakeCachePersistor{removeAllErr: errors.New("boom")}
	ep, err := evictnothing.New()
	assert.NoError(t, err)

	c, err := NewGenericCache(p, ep)
	assert.Nil(t, c)
	assert.EqualError(t, err, "boom")
	assert.Equal(t, 1, p.removeAllCalls)
}
