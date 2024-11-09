package ioutils

import (
	"fmt"
	"io"
)

type multiReadSeekCloser struct {
	currentReadOffset int64
	activeReaderIndex int
	readSeekClosers   []io.ReadSeekCloser
	size              int64
	readerSizeByIndex []int64
}

func NewMultiReadSeekCloser(readSeekClosers []io.ReadSeekCloser) (io.ReadSeekCloser, error) {
	readerSizeByIndex := []int64{}
	size := int64(0)
	for _, readSeekCloser := range readSeekClosers {
		currentOffset, err := readSeekCloser.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		n, err := readSeekCloser.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
		_, err = readSeekCloser.Seek(currentOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		readerSizeByIndex = append(readerSizeByIndex, n)
		size += n
	}

	return &multiReadSeekCloser{
		currentReadOffset: 0,
		activeReaderIndex: 0,
		readSeekClosers:   readSeekClosers,
		size:              size,
		readerSizeByIndex: readerSizeByIndex,
	}, nil
}

func (mrc *multiReadSeekCloser) Read(p []byte) (int, error) {
	if mrc.activeReaderIndex >= len(mrc.readSeekClosers) {
		return 0, io.EOF
	}
	n, err := mrc.readSeekClosers[mrc.activeReaderIndex].Read(p)
	if err == io.EOF && mrc.activeReaderIndex < len(mrc.readSeekClosers)-1 {
		mrc.activeReaderIndex += 1
		return mrc.Read(p)
	}
	mrc.currentReadOffset += int64(n)
	return n, err
}

func (mrc *multiReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		mrc.currentReadOffset = offset
	case io.SeekCurrent:
		mrc.currentReadOffset += offset
	case io.SeekEnd:
		mrc.currentReadOffset = mrc.size + offset
	}
	if mrc.currentReadOffset > mrc.size {
		return mrc.currentReadOffset, fmt.Errorf("error seeking")
	}
	if mrc.currentReadOffset < 0 {
		return mrc.currentReadOffset, fmt.Errorf("error seeking")
	}
	endOffset := int64(0)
	for readerIndex, readSeekCloser := range mrc.readSeekClosers {
		mrc.activeReaderIndex = readerIndex
		if endOffset <= mrc.currentReadOffset && mrc.currentReadOffset < endOffset+mrc.readerSizeByIndex[readerIndex] {
			break
		}
		n, err := readSeekCloser.Seek(0, io.SeekEnd)
		if err != nil {
			return mrc.currentReadOffset, err
		}
		endOffset += n
	}
	seekAmount := mrc.currentReadOffset
	if seekAmount >= endOffset {
		seekAmount -= endOffset
	}
	_, err := mrc.readSeekClosers[mrc.activeReaderIndex].Seek(seekAmount, io.SeekStart)
	if err != nil {
		return mrc.currentReadOffset, err
	}
	for _, readSeekCloser := range mrc.readSeekClosers[mrc.activeReaderIndex+1:] {
		_, err := readSeekCloser.Seek(0, io.SeekStart)
		if err != nil {
			return mrc.currentReadOffset, err
		}
	}
	return mrc.currentReadOffset, nil
}

func (mrc *multiReadSeekCloser) Close() error {
	var err error
	for _, readSeekCloser := range mrc.readSeekClosers {
		innerErr := readSeekCloser.Close()
		if err == nil && innerErr != nil {
			err = innerErr
		}
	}
	return err
}
