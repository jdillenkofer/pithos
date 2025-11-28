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

type CountingReader struct {
	Reader io.Reader
	Count  *int64
}

func NewCountingReader(reader io.Reader, count *int64) *CountingReader {
	return &CountingReader{
		Reader: reader,
		Count:  count,
	}
}

func (r *CountingReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	*r.Count += int64(n)
	return n, err
}
