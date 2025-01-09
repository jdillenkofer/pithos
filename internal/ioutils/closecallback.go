package ioutils

import "io"

type readAndCallbackCloser struct {
	innerReadCloser io.ReadCloser
	closeCallback   func() error
}

func NewReadAndCallbackCloser(innerReadCloser io.ReadCloser, closeCallback func() error) io.ReadCloser {
	return &readAndCallbackCloser{
		innerReadCloser: innerReadCloser,
		closeCallback:   closeCallback,
	}
}

func (r *readAndCallbackCloser) Read(p []byte) (int, error) {
	return r.innerReadCloser.Read(p)
}

func (r *readAndCallbackCloser) Close() error {
	err := r.closeCallback()
	if err != nil {
		return err
	}
	return nil
}
