package ioutils

import "io"

type MultiReadSeekCloser struct {
	currentReadOffset int64
	size              int64
	activeReaderIndex int
	readSeekClosers   []io.ReadSeekCloser
	readerSizeByIndex []int64
}

func NewMultiReadSeekCloser(readSeekClosers []io.ReadSeekCloser) (*MultiReadSeekCloser, error) {
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
	return &MultiReadSeekCloser{
		currentReadOffset: 0,
		size:              size,
		activeReaderIndex: 0,
		readSeekClosers:   readSeekClosers,
		readerSizeByIndex: readerSizeByIndex,
	}, nil
}

func (mrc *MultiReadSeekCloser) Read(p []byte) (int, error) {
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

func (mrc *MultiReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		mrc.currentReadOffset = offset
	case io.SeekCurrent:
		mrc.currentReadOffset += offset
	case io.SeekEnd:
		mrc.currentReadOffset = mrc.size + offset
	}
	if mrc.currentReadOffset > mrc.size {
		mrc.currentReadOffset = mrc.size
	}
	if mrc.currentReadOffset < 0 {
		mrc.currentReadOffset = 0
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

func (mrc *MultiReadSeekCloser) Close() error {
	var err error
	for _, readSeekCloser := range mrc.readSeekClosers {
		innerErr := readSeekCloser.Close()
		if err == nil && innerErr != nil {
			err = innerErr
		}
	}
	return err
}

type LimitedReadSeekCloser struct {
	limit               int64
	limitedReader       io.LimitedReader
	innerReadSeekCloser io.ReadSeekCloser
}

func NewLimitedReadSeekCloser(innerReadSeekCloser io.ReadSeekCloser, limit int64) *LimitedReadSeekCloser {
	return &LimitedReadSeekCloser{
		limit: limit,
		limitedReader: io.LimitedReader{
			R: innerReadSeekCloser,
			N: limit,
		},
		innerReadSeekCloser: innerReadSeekCloser,
	}
}

func (lrc *LimitedReadSeekCloser) Read(p []byte) (int, error) {
	n, err := lrc.limitedReader.Read(p)
	return n, err
}

func (lrc *LimitedReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	n, err := lrc.innerReadSeekCloser.Seek(offset, whence)
	lrc.limitedReader = io.LimitedReader{
		R: lrc.innerReadSeekCloser,
		N: lrc.limit - n,
	}
	if n > lrc.limit {
		return lrc.limit, err
	}
	return n, err
}

func (lrc *LimitedReadSeekCloser) Close() error {
	err := lrc.innerReadSeekCloser.Close()
	if err != nil {
		return err
	}
	return nil
}
