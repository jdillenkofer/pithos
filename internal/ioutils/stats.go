package ioutils

import (
	"io"
	"time"
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

// Deadline preserves a bounded stream's absolute deadline through metrics.
// A zero value means the inner reader has no lifetime bound.
func (s *statsReadCloser) Deadline() time.Time {
	if r, ok := s.innerReadCloser.(interface{ Deadline() time.Time }); ok {
		return r.Deadline()
	}
	return time.Time{}
}

func (s *statsReadCloser) Close() error {
	return s.innerReadCloser.Close()
}

type statsReadSeekCloser struct {
	statsReadCloser
	innerSeeker io.Seeker
}

// NewStatsReadSeekCloser is NewStatsReadCloser for readers that also support
// seeking; the wrapper preserves the Seek method so callers can still
// discover it via type assertion (e.g. SkipNBytes for range reads).
func NewStatsReadSeekCloser(innerReadSeekCloser io.ReadSeekCloser, readCallback func(n int)) io.ReadSeekCloser {
	return &statsReadSeekCloser{
		statsReadCloser: statsReadCloser{
			innerReadCloser: innerReadSeekCloser,
			readCallback:    readCallback,
		},
		innerSeeker: innerReadSeekCloser,
	}
}

func (s *statsReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return s.innerSeeker.Seek(offset, whence)
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
