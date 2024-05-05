package ioutils

import (
	"fmt"
	"io"
)

type LimitedStartReadSeekCloser struct {
	isInitialized       bool
	currentReadOffset   int64
	startLimit          int64
	size                int64
	innerReadSeekCloser io.ReadSeekCloser
}

func NewLimitedStartReadSeekCloser(innerReadSeekCloser io.ReadSeekCloser, startLimit int64) *LimitedStartReadSeekCloser {
	return &LimitedStartReadSeekCloser{
		isInitialized:       false,
		startLimit:          startLimit,
		currentReadOffset:   -1,
		size:                -1,
		innerReadSeekCloser: innerReadSeekCloser,
	}
}

func (lrc *LimitedStartReadSeekCloser) maybeInitialize() error {
	if !lrc.isInitialized {
		var err error
		lrc.size, err = lrc.innerReadSeekCloser.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		lrc.currentReadOffset, err = lrc.innerReadSeekCloser.Seek(lrc.startLimit, io.SeekStart)
		if err != nil {
			return err
		}
		lrc.isInitialized = true
	}
	return nil
}

func (lrc *LimitedStartReadSeekCloser) Read(p []byte) (int, error) {
	err := lrc.maybeInitialize()
	if err != nil {
		return 0, err
	}
	n, err := lrc.innerReadSeekCloser.Read(p)
	lrc.currentReadOffset += int64(n)
	return n, err
}

func (lrc *LimitedStartReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	err := lrc.maybeInitialize()
	if err != nil {
		return 0, err
	}
	switch whence {
	case io.SeekStart:
		offset += lrc.startLimit
	case io.SeekCurrent:
		offset += lrc.currentReadOffset
		whence = io.SeekStart
	case io.SeekEnd:
		offset = lrc.size + offset
		whence = io.SeekStart
	}
	if offset < lrc.startLimit {
		return lrc.currentReadOffset - lrc.startLimit, fmt.Errorf("error seeking")
	}
	if offset > lrc.size {
		return lrc.currentReadOffset - lrc.startLimit, fmt.Errorf("error seeking")
	}
	n, err := lrc.innerReadSeekCloser.Seek(offset, whence)
	lrc.currentReadOffset = n
	return n - lrc.startLimit, err
}

func (lrc *LimitedStartReadSeekCloser) Close() error {
	err := lrc.innerReadSeekCloser.Close()
	if err != nil {
		return err
	}
	return nil
}

type LimitedEndReadSeekCloser struct {
	isInitialized       bool
	currentReadOffset   int64
	endLimit            int64
	size                int64
	innerReadSeekCloser io.ReadSeekCloser
}

func NewLimitedEndReadSeekCloser(innerReadSeekCloser io.ReadSeekCloser, endLimit int64) *LimitedEndReadSeekCloser {
	return &LimitedEndReadSeekCloser{
		isInitialized:       false,
		endLimit:            endLimit,
		currentReadOffset:   -1,
		size:                -1,
		innerReadSeekCloser: innerReadSeekCloser,
	}
}

func (lrc *LimitedEndReadSeekCloser) maybeInitialize() error {
	if !lrc.isInitialized {
		offset, err := lrc.innerReadSeekCloser.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		lrc.size, err = lrc.innerReadSeekCloser.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		lrc.currentReadOffset, err = lrc.innerReadSeekCloser.Seek(offset, io.SeekStart)
		if err != nil {
			return err
		}
		lrc.isInitialized = true
	}
	return nil
}

func (lrc *LimitedEndReadSeekCloser) Read(p []byte) (int, error) {
	err := lrc.maybeInitialize()
	if err != nil {
		return 0, err
	}
	if lrc.currentReadOffset >= lrc.endLimit {
		return 0, io.EOF
	}
	n, err := lrc.innerReadSeekCloser.Read(p)
	lrc.currentReadOffset += int64(n)
	if lrc.currentReadOffset >= lrc.endLimit {
		n -= int(lrc.currentReadOffset - lrc.endLimit)
		lrc.currentReadOffset = lrc.endLimit
		_, err = lrc.innerReadSeekCloser.Seek(lrc.currentReadOffset, io.SeekStart)
		if err != nil {
			return 0, err
		}
	}
	return n, err
}

func (lrc *LimitedEndReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	err := lrc.maybeInitialize()
	if err != nil {
		return 0, err
	}
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += lrc.currentReadOffset
		whence = io.SeekStart
	case io.SeekEnd:
		end := lrc.size
		if end > lrc.endLimit {
			end = lrc.endLimit
		}
		offset = end + offset
		whence = io.SeekStart
	}
	if offset > lrc.endLimit {
		return lrc.currentReadOffset, fmt.Errorf("error seeking")
	}
	if offset < 0 {
		return lrc.currentReadOffset, fmt.Errorf("error seeking")
	}
	n, err := lrc.innerReadSeekCloser.Seek(offset, whence)
	lrc.currentReadOffset = n
	return n, err
}

func (lrc *LimitedEndReadSeekCloser) Close() error {
	err := lrc.innerReadSeekCloser.Close()
	if err != nil {
		return err
	}
	return nil
}
