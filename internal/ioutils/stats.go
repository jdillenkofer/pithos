package ioutils

import (
	"io"
)

type StatsReadSeekCloser struct {
	innerReadSeekCloser io.ReadSeekCloser
	readCallback        func(n int)
}

func NewStatsReadSeekCloser(innerReadSeekCloser io.ReadSeekCloser, readCallback func(n int)) *StatsReadSeekCloser {
	return &StatsReadSeekCloser{
		innerReadSeekCloser: innerReadSeekCloser,
		readCallback:        readCallback,
	}
}

func (s *StatsReadSeekCloser) Read(p []byte) (int, error) {
	n, err := s.innerReadSeekCloser.Read(p)
	if s.readCallback != nil {
		s.readCallback(n)
	}
	return n, err
}

func (s *StatsReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return s.innerReadSeekCloser.Seek(offset, whence)
}

func (s *StatsReadSeekCloser) Close() error {
	return s.innerReadSeekCloser.Close()
}
