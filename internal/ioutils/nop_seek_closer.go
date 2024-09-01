package ioutils

import (
	"io"
)

type NopSeekCloser struct {
	innerReader io.Reader
}

func NewNopSeekCloser(innerReader io.Reader) *NopSeekCloser {
	return &NopSeekCloser{
		innerReader: innerReader,
	}
}

func (nsc *NopSeekCloser) Read(p []byte) (int, error) {
	return nsc.innerReader.Read(p)
}

func (nsc *NopSeekCloser) Seek(offset int64, whence int) (int64, error) {
	panic("Called Seek in NopSeekCloser")
}

func (nsc *NopSeekCloser) Close() error {
	panic("Called Close in NopSeekCloser")
}
