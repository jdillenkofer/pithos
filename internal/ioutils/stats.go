package ioutils

import (
	"io"
)

type statsReadSeekCloser struct {
	innerReadSeekCloser io.ReadSeekCloser
	readCallback        func(n int)
}

func NewStatsReadSeekCloser(innerReadSeekCloser io.ReadSeekCloser, readCallback func(n int)) io.ReadSeekCloser {
	return &statsReadSeekCloser{
		innerReadSeekCloser: innerReadSeekCloser,
		readCallback:        readCallback,
	}
}

func (s *statsReadSeekCloser) Read(p []byte) (int, error) {
	n, err := s.innerReadSeekCloser.Read(p)
	if s.readCallback != nil {
		s.readCallback(n)
	}
	return n, err
}

func (s *statsReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return s.innerReadSeekCloser.Seek(offset, whence)
}

func (s *statsReadSeekCloser) Close() error {
	return s.innerReadSeekCloser.Close()
}
