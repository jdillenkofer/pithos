package ioutils

import (
	"io"
)

func SkipNBytes(r io.Reader, n int64) (int64, error) {
	var err error
	switch r := r.(type) {
	case io.Seeker:
		_, err = r.Seek(n, io.SeekCurrent)
	default:
		_, err = io.CopyN(io.Discard, r, n)
	}
	return n, err
}

func SkipAllBytes(r io.Reader) (n int64, err error) {
	switch r := r.(type) {
	case io.Seeker:
		n, err = r.Seek(0, io.SeekEnd)
	default:
		n, err = io.Copy(io.Discard, r)
	}
	return n, err
}

type limitedEndReadCloser struct {
	io.Reader
	innerReadCloser io.ReadCloser
}

func NewLimitedEndReadCloser(innerReadCloser io.ReadCloser, endLimit int64) io.ReadCloser {
	return &limitedEndReadCloser{
		io.LimitReader(innerReadCloser, endLimit),
		innerReadCloser,
	}
}

func (lrc *limitedEndReadCloser) Close() error {
	err := lrc.innerReadCloser.Close()
	if err != nil {
		return err
	}
	return nil
}
