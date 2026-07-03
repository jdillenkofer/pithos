package ioutils

import (
	"bytes"
	"io"
	mathrand "math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type seekableBuffer struct {
	*bytes.Reader
	closed bool
}

func (s *seekableBuffer) Close() error {
	s.closed = true
	return nil
}

func newSeekableBuffer(data []byte) *seekableBuffer {
	return &seekableBuffer{Reader: bytes.NewReader(data)}
}

func TestReadAheadSequentialMatches(t *testing.T) {
	data := make([]byte, 1000*1000+37)
	rng := mathrand.New(mathrand.NewSource(1))
	rng.Read(data)

	for _, blockSize := range []int{1, 7, 4096, 128 * 1024, len(data) + 100} {
		r := NewReadAheadReadSeekCloser(newSeekableBuffer(data), blockSize)
		got, err := io.ReadAll(r)
		require.NoError(t, err, "blockSize=%d", blockSize)
		assert.True(t, bytes.Equal(data, got), "blockSize=%d", blockSize)
		require.NoError(t, r.Close())
	}
}

func TestReadAheadRandomSeeks(t *testing.T) {
	data := make([]byte, 300*1024)
	rng := mathrand.New(mathrand.NewSource(2))
	rng.Read(data)

	r := NewReadAheadReadSeekCloser(newSeekableBuffer(data), 8192)
	defer r.Close()

	for i := 0; i < 200; i++ {
		off := rng.Int63n(int64(len(data)))
		whence := io.SeekStart
		pos, err := r.Seek(off, whence)
		require.NoError(t, err)
		require.Equal(t, off, pos)

		readLen := int(rng.Int63n(20000)) + 1
		want := data[off:min(int(off)+readLen, len(data))]
		got := make([]byte, len(want))
		_, err = io.ReadFull(r, got)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(want, got), "off=%d len=%d", off, readLen)
	}
}

func TestReadAheadSeekEndAndEOF(t *testing.T) {
	data := []byte("0123456789")
	r := NewReadAheadReadSeekCloser(newSeekableBuffer(data), 4)
	defer r.Close()

	pos, err := r.Seek(-3, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(7), pos)
	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "789", string(rest))

	// Reads at and past EOF return EOF.
	_, err = r.Seek(int64(len(data)), io.SeekStart)
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = r.Read(buf)
	assert.Equal(t, io.EOF, err)

	_, err = r.Seek(int64(len(data))+100, io.SeekStart)
	require.NoError(t, err)
	_, err = r.Read(buf)
	assert.Equal(t, io.EOF, err)
}

func TestReadAheadSeekCurrent(t *testing.T) {
	data := []byte("abcdefghijklmnopqrstuvwxyz")
	r := NewReadAheadReadSeekCloser(newSeekableBuffer(data), 5)
	defer r.Close()

	buf := make([]byte, 3)
	_, err := io.ReadFull(r, buf)
	require.NoError(t, err)
	assert.Equal(t, "abc", string(buf))

	pos, err := r.Seek(4, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(7), pos)
	_, err = io.ReadFull(r, buf)
	require.NoError(t, err)
	assert.Equal(t, "hij", string(buf))
}

func TestReadAheadCloseReleasesInner(t *testing.T) {
	data := make([]byte, 100)
	inner := newSeekableBuffer(data)
	r := NewReadAheadReadSeekCloser(inner, 16)
	buf := make([]byte, 10)
	_, err := r.Read(buf)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	assert.True(t, inner.closed)

	_, err = r.Read(buf)
	assert.Error(t, err)
	require.NoError(t, r.Close())
}
