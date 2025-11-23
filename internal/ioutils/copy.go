package ioutils

import (
	"io"
)

// CopyN copies n bytes (or until an error) from src to dst.
// It uses io.CopyBuffer with a 128KB buffer.
func CopyN(dst io.Writer, src io.Reader, n int64) (written int64, err error) {
	buf := make([]byte, 128*1024)
	written, err = io.CopyBuffer(dst, io.LimitReader(src, n), buf)
	if written == n {
		return n, nil
	}
	if written < n && err == nil {
		// src stopped early; must have been EOF.
		err = io.EOF
	}
	return
}

// Copy copies from src to dst until either EOF is reached on src or an error occurs.
// It uses io.CopyBuffer with a 128KB buffer.
func Copy(dst io.Writer, src io.Reader) (written int64, err error) {
	buf := make([]byte, 128*1024)
	return io.CopyBuffer(dst, src, buf)
}
