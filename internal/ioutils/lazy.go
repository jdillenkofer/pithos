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

type lazyReadSeekCloser struct {
	init           func() (io.ReadSeekCloser, error)
	readSeekCloser io.ReadSeekCloser
	err            error
}

func (r *lazyReadSeekCloser) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.readSeekCloser == nil {
		readSeekCloser, err := r.init()
		if err != nil {
			r.err = err
			return 0, err
		}
		r.readSeekCloser = readSeekCloser
	}
	return r.readSeekCloser.Read(p)
}

func (r *lazyReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.readSeekCloser == nil {
		readSeekCloser, err := r.init()
		if err != nil {
			r.err = err
			return 0, err
		}
		r.readSeekCloser = readSeekCloser
	}
	return r.readSeekCloser.Seek(offset, whence)
}

func (r *lazyReadSeekCloser) Close() error {
	if r.readSeekCloser != nil {
		return r.readSeekCloser.Close()
	}
	return nil
}

func NewLazyReadSeekCloser(init func() (io.ReadSeekCloser, error)) io.ReadSeekCloser {
	return &lazyReadSeekCloser{
		init:           init,
		readSeekCloser: nil,
		err:            nil,
	}
}
