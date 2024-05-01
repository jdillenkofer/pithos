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
	multiReadSeekCloser, err := NewMultiReadSeekCloser([]io.ReadSeekCloser{&reader})
	assert.Nil(t, err)
	err = iotest.TestReader(multiReadSeekCloser, content)
	assert.Nil(t, err)
}

func TestDualMultiReadSeekCloser(t *testing.T) {
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}
	reader := byteReadSeekCloser{
		bytes.NewReader(content[0:5]),
	}
	reader2 := byteReadSeekCloser{
		bytes.NewReader(content[5:11]),
	}
	multiReadSeekCloser, err := NewMultiReadSeekCloser([]io.ReadSeekCloser{&reader, &reader2})
	assert.Nil(t, err)
	err = iotest.TestReader(multiReadSeekCloser, content)
	assert.Nil(t, err)
}

func TestTripleMultiReadSeekCloser(t *testing.T) {
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}
	reader := byteReadSeekCloser{
		bytes.NewReader(content[0:5]),
	}
	reader2 := byteReadSeekCloser{
		bytes.NewReader(content[5:7]),
	}
	reader3 := byteReadSeekCloser{
		bytes.NewReader(content[7:11]),
	}
	multiReadSeekCloser, err := NewMultiReadSeekCloser([]io.ReadSeekCloser{&reader, &reader2, &reader3})
	assert.Nil(t, err)
	err = iotest.TestReader(multiReadSeekCloser, content)
	assert.Nil(t, err)
}

func TestQuadMultiReadSeekCloser(t *testing.T) {
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}
	reader := byteReadSeekCloser{
		bytes.NewReader(content[0:5]),
	}
	reader2 := byteReadSeekCloser{
		bytes.NewReader(content[5:7]),
	}
	reader3 := byteReadSeekCloser{
		bytes.NewReader(content[7:8]),
	}
	reader4 := byteReadSeekCloser{
		bytes.NewReader(content[8:11]),
	}
	multiReadSeekCloser, err := NewMultiReadSeekCloser([]io.ReadSeekCloser{&reader, &reader2, &reader3, &reader4})
	assert.Nil(t, err)
	err = iotest.TestReader(multiReadSeekCloser, content)
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
