package ioutils

import "io"

type MultiReadSeekCloser struct {
	activeReaderIndex int
	readSeekClosers   []io.ReadSeekCloser
}

func NewMultiReadSeekCloser(readSeekClosers []io.ReadSeekCloser) *MultiReadSeekCloser {
	return &MultiReadSeekCloser{
		activeReaderIndex: 0,
		readSeekClosers:   readSeekClosers,
	}
}

func (mrc *MultiReadSeekCloser) Read(p []byte) (n int, err error) {
	if mrc.activeReaderIndex >= len(mrc.readSeekClosers) {
		return 0, io.EOF
	}
	n, err = mrc.readSeekClosers[mrc.activeReaderIndex].Read(p)
	if err == io.EOF && mrc.activeReaderIndex < len(mrc.readSeekClosers)-1 {
		mrc.activeReaderIndex += 1
		return mrc.Read(p)
	}
	return n, err
}

func (mrc *MultiReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	var n int64
	var err error
	switch whence {
	case io.SeekStart:
		mrc.activeReaderIndex = 0
		n, err = mrc.readSeekClosers[mrc.activeReaderIndex].Seek(offset, whence)
	case io.SeekCurrent:
		n, err = mrc.readSeekClosers[mrc.activeReaderIndex].Seek(offset, whence)
	case io.SeekEnd:
		mrc.activeReaderIndex = len(mrc.readSeekClosers) - 1
		n, err = mrc.readSeekClosers[mrc.activeReaderIndex].Seek(offset, whence)
	}
	return n, err
}

func (mrc *MultiReadSeekCloser) Close() error {
	for _, readSeekCloser := range mrc.readSeekClosers {
		err := readSeekCloser.Close()
		if err != nil {
			return err
		}
	}
	return nil
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
