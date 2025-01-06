package ioutils

import (
	"io"
)

type statsReadCloser struct {
	innerReadCloser io.ReadCloser
	readCallback    func(n int)
}

func NewStatsReadCloser(innerReadCloser io.ReadCloser, readCallback func(n int)) io.ReadCloser {
	return &statsReadCloser{
		innerReadCloser: innerReadCloser,
		readCallback:    readCallback,
	}
}

func (s *statsReadCloser) Read(p []byte) (int, error) {
	n, err := s.innerReadCloser.Read(p)
	if s.readCallback != nil {
		s.readCallback(n)
	}
	return n, err
}

func (s *statsReadCloser) Close() error {
	return s.innerReadCloser.Close()
}
