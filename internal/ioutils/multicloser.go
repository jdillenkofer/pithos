package ioutils

import "io"

type MultiCloser struct {
	closers []io.Closer
}

func NewMultiCloser(closers []io.Closer) *MultiCloser {
	return &MultiCloser{
		closers: closers,
	}
}

func (m *MultiCloser) Close() error {
	var err error
	for _, c := range m.closers {
		if e := c.Close(); e != nil {
			err = e
		}
	}
	return err
}
