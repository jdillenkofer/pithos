package ioutils

import (
	"bytes"
	"io"
	"os"
)

const diskCachePattern = "pithos-disk-cache-*"

type diskCachedReadSeekCloser struct {
	f *os.File
}

// NewSmartCachedReadSeekCloser reads the content of r.
// If the content is smaller than memoryLimit, it returns a ReadSeekCloser backed by memory.
// If the content is larger than memoryLimit, it writes the content to a temporary file
// and returns a ReadSeekCloser that reads from that file.
func NewSmartCachedReadSeekCloser(r io.Reader, memoryLimit int64) (io.ReadSeekCloser, error) {
	// Try to read one byte more than the limit to check if we exceed it
	limitedReader := io.LimitReader(r, memoryLimit+1)
	data, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, err
	}

	if int64(len(data)) <= memoryLimit {
		return NewByteReadSeekCloser(data), nil
	}

	// We exceeded the memory limit, switch to disk cache
	f, err := os.CreateTemp("", diskCachePattern)
	if err != nil {
		return nil, err
	}

	// Write what we have already read
	if _, err := io.Copy(f, bytes.NewReader(data)); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}

	// Copy the rest of the stream
	if _, err := Copy(f, r); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}

	return &diskCachedReadSeekCloser{f: f}, nil
}

// NewDiskCachedReadSeekCloser reads the entire content of r into a temporary file
// and returns a ReadSeekCloser that reads from that file.
// Closing the returned ReadSeekCloser will close and remove the temporary file.
func NewDiskCachedReadSeekCloser(r io.Reader) (io.ReadSeekCloser, error) {
	f, err := os.CreateTemp("", diskCachePattern)
	if err != nil {
		return nil, err
	}

	if _, err := Copy(f, r); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}

	return &diskCachedReadSeekCloser{f: f}, nil
}

func (d *diskCachedReadSeekCloser) Read(p []byte) (n int, err error) {
	return d.f.Read(p)
}

func (d *diskCachedReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return d.f.Seek(offset, whence)
}

func (d *diskCachedReadSeekCloser) Close() error {
	filename := d.f.Name()
	err := d.f.Close()
	if removeErr := os.Remove(filename); removeErr != nil && err == nil {
		err = removeErr
	}
	return err
}
