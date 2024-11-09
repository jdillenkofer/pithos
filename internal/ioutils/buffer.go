package ioutils

import (
	"bytes"
	"io"
)

func NewByteReadSeekCloser(buffer []byte) io.ReadSeekCloser {
	return byteReadSeekCloser{
		bytes.NewReader(buffer),
	}
}

type byteReadSeekCloser struct {
	io.ReadSeeker
}

func (brsc byteReadSeekCloser) Close() error {
	return nil
}
