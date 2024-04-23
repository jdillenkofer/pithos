package ioutils

import "io"

type MultiReadCloser struct {
	multiReader io.Reader
	readClosers []io.ReadCloser
}

func NewMultiReadCloser(readClosers []io.ReadCloser) *MultiReadCloser {
	readers := []io.Reader{}
	for _, readCloser := range readClosers {
		readers = append(readers, readCloser)
	}
	return &MultiReadCloser{
		multiReader: io.MultiReader(readers...),
		readClosers: readClosers,
	}
}

func (mrc *MultiReadCloser) Read(p []byte) (n int, err error) {
	return mrc.multiReader.Read(p)
}

func (mrc *MultiReadCloser) Close() error {
	for _, readCloser := range mrc.readClosers {
		err := readCloser.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

type LimitedReadCloser struct {
	limitedReader   io.LimitedReader
	innerReadCloser io.ReadCloser
}

func NewLimitedReadCloser(innerReadCloser io.ReadCloser, limit int64) *LimitedReadCloser {
	return &LimitedReadCloser{
		limitedReader: io.LimitedReader{
			R: innerReadCloser,
			N: limit,
		},
		innerReadCloser: innerReadCloser,
	}
}

func (lrc *LimitedReadCloser) Read(p []byte) (n int, err error) {
	return lrc.limitedReader.Read(p)
}

func (lrc *LimitedReadCloser) Close() error {
	err := lrc.innerReadCloser.Close()
	if err != nil {
		return err
	}
	return nil
}
