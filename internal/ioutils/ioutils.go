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
