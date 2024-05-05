package ioutils

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
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

func TestDualMultiReadSeekCloserWithStartOffset(t *testing.T) {
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}
	reader := byteReadSeekCloser{
		bytes.NewReader(content[0:5]),
	}
	var reader2 io.ReadSeekCloser
	reader2 = &byteReadSeekCloser{
		bytes.NewReader(content[5:11]),
	}
	reader2 = NewLimitedStartReadSeekCloser(reader2, 1)
	multiReadSeekCloser, err := NewMultiReadSeekCloser([]io.ReadSeekCloser{&reader, reader2})
	assert.Nil(t, err)
	newContent := []byte{}
	newContent = append(newContent, content[0:5]...)
	newContent = append(newContent, content[6:11]...)
	err = iotest.TestReader(multiReadSeekCloser, newContent)
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

func TestSimpleLimitedStartReadSeekCloser(t *testing.T) {
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f'}
	reader := byteReadSeekCloser{
		bytes.NewReader(content),
	}
	limitedStartReadSeekCloser := NewLimitedStartReadSeekCloser(&reader, 2)
	err := iotest.TestReader(limitedStartReadSeekCloser, content[2:])
	assert.Nil(t, err)
}

func TestSimpleLimitedEndReadSeekCloser(t *testing.T) {
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f'}
	reader := byteReadSeekCloser{
		bytes.NewReader(content),
	}
	limitedEndReadSeekCloser := NewLimitedEndReadSeekCloser(&reader, 2)
	err := iotest.TestReader(limitedEndReadSeekCloser, content[0:2])
	assert.Nil(t, err)
}

func TestSimpleDebugReadSeekCloser(t *testing.T) {
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f'}
	reader := byteReadSeekCloser{
		bytes.NewReader(content),
	}
	debugReadSeekCloser := NewDebugReadSeekCloser("Debug", &reader)
	err := iotest.TestReader(debugReadSeekCloser, content)
	assert.Nil(t, err)
}
