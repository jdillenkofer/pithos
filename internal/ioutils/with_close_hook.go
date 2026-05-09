package ioutils

import "io"

type readCloserWithCloseHook struct {
	inner   io.ReadCloser
	onClose func() error
}

func NewReadCloserWithCloseHook(inner io.ReadCloser, onClose func() error) io.ReadCloser {
	return &readCloserWithCloseHook{inner: inner, onClose: onClose}
}

func (r *readCloserWithCloseHook) Read(p []byte) (int, error) {
	return r.inner.Read(p)
}

func (r *readCloserWithCloseHook) Close() error {
	err := r.inner.Close()
	if r.onClose == nil {
		return err
	}
	if closeErr := r.onClose(); err == nil {
		err = closeErr
	}
	return err
}
