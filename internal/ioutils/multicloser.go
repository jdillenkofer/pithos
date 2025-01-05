package ioutils

import "io"

type multiCloser struct {
	closers []io.Closer
}

func NewMultiCloser(closers []io.Closer) io.Closer {
	return &multiCloser{
		closers: closers,
	}
}

func (m *multiCloser) Close() error {
	var err error
	for _, c := range m.closers {
		if e := c.Close(); e != nil {
			err = e
		}
	}
	return err
}
