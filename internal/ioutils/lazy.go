package ioutils

import "io"

type lazyReadCloser struct {
	init       func() (io.ReadCloser, error)
	readCloser io.ReadCloser
	err        error
}

func (r *lazyReadCloser) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.readCloser == nil {
		readCloser, err := r.init()
		if err != nil {
			r.err = err
			return 0, err
		}
		r.readCloser = readCloser
	}
	return r.readCloser.Read(p)
}

func (r *lazyReadCloser) Close() error {
	if r.readCloser != nil {
		return r.readCloser.Close()
	}
	return nil
}

func NewLazyReadCloser(init func() (io.ReadCloser, error)) io.ReadCloser {
	return &lazyReadCloser{
		init:       init,
		readCloser: nil,
		err:        nil,
	}
}
