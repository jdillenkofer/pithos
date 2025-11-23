package ioutils

import (
	"io"
	"os"
)

type diskCachedReadSeekCloser struct {
	f *os.File
}

// NewDiskCachedReadSeekCloser reads the entire content of r into a temporary file
// and returns a ReadSeekCloser that reads from that file.
// Closing the returned ReadSeekCloser will close and remove the temporary file.
func NewDiskCachedReadSeekCloser(r io.Reader) (io.ReadSeekCloser, error) {
	f, err := os.CreateTemp("", "pithos-disk-cache-*")
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
