package ioutils

import (
	"testing"
	"testing/iotest"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestSimpleMultiReadSeekCloser(t *testing.T) {
	testutils.SkipIfIntegration(t)
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f'}
	reader := NewByteReadSeekCloser(content)
	multiReadCloser := NewMultiReadCloser(reader)
	err := iotest.TestReader(multiReadCloser, content)
	assert.Nil(t, err)
}

func TestDualMultiReadSeekCloser(t *testing.T) {
	testutils.SkipIfIntegration(t)
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}
	reader := NewByteReadSeekCloser(content[0:5])
	reader2 := NewByteReadSeekCloser(content[5:11])
	multiReadCloser := NewMultiReadCloser(reader, reader2)
	err := iotest.TestReader(multiReadCloser, content)
	assert.Nil(t, err)
}

func TestTripleMultiReadSeekCloser(t *testing.T) {
	testutils.SkipIfIntegration(t)
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}
	reader := NewByteReadSeekCloser(content[0:5])
	reader2 := NewByteReadSeekCloser(content[5:7])
	reader3 := NewByteReadSeekCloser(content[7:11])
	multiReadCloser := NewMultiReadCloser(reader, reader2, reader3)
	err := iotest.TestReader(multiReadCloser, content)
	assert.Nil(t, err)
}

func TestQuadMultiReadSeekCloser(t *testing.T) {
	testutils.SkipIfIntegration(t)
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}
	reader := NewByteReadSeekCloser(content[0:5])
	reader2 := NewByteReadSeekCloser(content[5:7])
	reader3 := NewByteReadSeekCloser(content[7:8])
	reader4 := NewByteReadSeekCloser(content[8:11])
	multiReadCloser := NewMultiReadCloser(reader, reader2, reader3, reader4)
	err := iotest.TestReader(multiReadCloser, content)
	assert.Nil(t, err)
}

func TestSimpleLimitedEndReadSeekCloser(t *testing.T) {
	testutils.SkipIfIntegration(t)
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f'}
	reader := NewByteReadSeekCloser(content)
	limitedEndReadSeekCloser := NewLimitedEndReadCloser(reader, 2)
	err := iotest.TestReader(limitedEndReadSeekCloser, content[0:2])
	assert.Nil(t, err)
}

func TestSimpleDebugReadCloser(t *testing.T) {
	testutils.SkipIfIntegration(t)
	content := []byte{'a', 'b', 'c', 'd', 'e', 'f'}
	reader := NewByteReadSeekCloser(content)
	debugReadCloser := NewDebugReadCloser("Debug", reader)
	err := iotest.TestReader(debugReadCloser, content)
	assert.Nil(t, err)
}
