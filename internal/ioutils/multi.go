package ioutils

import (
	"io"
)

type multiReadCloser struct {
	io.Reader
	readClosers []io.ReadCloser
}

func NewMultiReadCloser(readClosers ...io.ReadCloser) io.ReadCloser {
	var readers []io.Reader
	for _, readCloser := range readClosers {
		readers = append(readers, readCloser)
	}
	multiReader := io.MultiReader(readers...)
	return &multiReadCloser{
		multiReader,
		readClosers,
	}
}

func (mrc *multiReadCloser) Close() error {
	var err error
	for _, readSeekCloser := range mrc.readClosers {
		innerErr := readSeekCloser.Close()
		if err == nil && innerErr != nil {
			err = innerErr
		}
	}
	return err
}
