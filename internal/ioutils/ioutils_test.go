package ioutils

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"testing/iotest"
)

type byteReadSeekCloser struct {
	io.ReadSeeker
}

func (brsc *byteReadSeekCloser) Close() error {
	return nil
}

func TestSimpleMultiReadSeekCloser(t *testing.T) {
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f'}
	reader := byteReadSeekCloser{
		bytes.NewReader(content),
	}
	multiReadSeekCloser := NewMultiReadSeekCloser([]io.ReadSeekCloser{&reader})
	err := iotest.TestReader(multiReadSeekCloser, content)
	assert.Nil(t, err)
}

func TestComplexMultiReadSeekCloser(t *testing.T) {
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}
	reader := byteReadSeekCloser{
		bytes.NewReader(content[0:5]),
	}
	reader2 := byteReadSeekCloser{
		bytes.NewReader(content[5:11]),
	}
	multiReadSeekCloser := NewMultiReadSeekCloser([]io.ReadSeekCloser{&reader, &reader2})
	err := iotest.TestReader(multiReadSeekCloser, content)
	assert.Nil(t, err)
}

func TestSimpleLimitedReadSeekCloser(t *testing.T) {
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f'}
	reader := byteReadSeekCloser{
		bytes.NewReader(content),
	}
	limitedReadSeekCloser := NewLimitedReadSeekCloser(&reader, 2)
	err := iotest.TestReader(limitedReadSeekCloser, content[0:2])
	assert.Nil(t, err)
}
