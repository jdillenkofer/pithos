package ioutils

import (
	"io"
	"sync"
)

const copyBufferSize = 128 * 1024

// copyBufferPool recycles the copy buffers across calls. Copy/CopyN sit on
// every object read and write path, so allocating a fresh 128KB buffer per
// call causes measurable GC churn under load.
var copyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, copyBufferSize)
		return &buf
	},
}

// CopyN copies n bytes (or until an error) from src to dst.
// It uses io.CopyBuffer with a pooled 128KB buffer.
func CopyN(dst io.Writer, src io.Reader, n int64) (written int64, err error) {
	buf := copyBufferPool.Get().(*[]byte)
	defer copyBufferPool.Put(buf)
	written, err = io.CopyBuffer(dst, io.LimitReader(src, n), *buf)
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
// It uses io.CopyBuffer with a pooled 128KB buffer.
func Copy(dst io.Writer, src io.Reader) (written int64, err error) {
	buf := copyBufferPool.Get().(*[]byte)
	defer copyBufferPool.Put(buf)
	return io.CopyBuffer(dst, src, *buf)
}
