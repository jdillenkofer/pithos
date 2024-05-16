package ioutils

import (
	"bytes"
	"io"
)

func NewByteReadSeekCloser(buffer []byte) ByteReadSeekCloser {
	return ByteReadSeekCloser{
		bytes.NewReader(buffer),
	}
}

type ByteReadSeekCloser struct {
	io.ReadSeeker
}

func (brsc ByteReadSeekCloser) Close() error {
	return nil
}
