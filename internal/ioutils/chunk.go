package ioutils

import "io"

const readChunkInitialCap = 128 * 1024 // 128KB

// ReadChunk reads up to max bytes from r into a freshly allocated, caller-owned
// slice. The buffer grows geometrically up to max so small reads don't allocate
// the full chunk size. It returns io.EOF once r is exhausted; a nil error means
// max bytes were read and more may remain.
func ReadChunk(r io.Reader, max int) ([]byte, error) {
	buf := make([]byte, 0, min(readChunkInitialCap, max))
	for len(buf) < max {
		if len(buf) == cap(buf) {
			newCap := min(cap(buf)*2, max)
			grown := make([]byte, len(buf), newCap)
			copy(grown, buf)
			buf = grown
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err != nil {
			return buf, err
		}
	}
	return buf, nil
}
