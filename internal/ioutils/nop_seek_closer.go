package ioutils

import (
	"io"
)

type nopSeekCloser struct {
	innerReader io.Reader
}

func NewNopSeekCloser(innerReader io.Reader) io.ReadSeekCloser {
	return &nopSeekCloser{
		innerReader: innerReader,
	}
}

func (nsc *nopSeekCloser) Read(p []byte) (int, error) {
	return nsc.innerReader.Read(p)
}

func (nsc *nopSeekCloser) Seek(offset int64, whence int) (int64, error) {
	panic("Called Seek in NopSeekCloser")
}

func (nsc *nopSeekCloser) Close() error {
	panic("Called Close in NopSeekCloser")
}
