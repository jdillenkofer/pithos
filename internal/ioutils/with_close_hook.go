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

type readSeekCloserWithCloseHook struct {
	inner   io.ReadSeekCloser
	onClose func() error
}

// NewReadSeekCloserWithCloseHook is NewReadCloserWithCloseHook for readers
// that also support seeking; the wrapper preserves the Seek method so
// callers can still discover it via type assertion.
func NewReadSeekCloserWithCloseHook(inner io.ReadSeekCloser, onClose func() error) io.ReadSeekCloser {
	return &readSeekCloserWithCloseHook{inner: inner, onClose: onClose}
}

func (r *readSeekCloserWithCloseHook) Read(p []byte) (int, error) {
	return r.inner.Read(p)
}

func (r *readSeekCloserWithCloseHook) Seek(offset int64, whence int) (int64, error) {
	return r.inner.Seek(offset, whence)
}

func (r *readSeekCloserWithCloseHook) Close() error {
	err := r.inner.Close()
	if r.onClose == nil {
		return err
	}
	if closeErr := r.onClose(); err == nil {
		err = closeErr
	}
	return err
}
