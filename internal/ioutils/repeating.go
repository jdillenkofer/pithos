package ioutils

import (
	"io"
)

type repeatingReader struct {
	data   []byte
	offset int
}

// NewRepeatingReader returns an io.Reader that infinitely repeats the given data.
func NewRepeatingReader(data []byte) io.Reader {
	if len(data) == 0 {
		return &repeatingReader{data: []byte{0}, offset: 0}
	}
	return &repeatingReader{
		data:   data,
		offset: 0,
	}
}

func (r *repeatingReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	for n < len(p) {
		copied := copy(p[n:], r.data[r.offset:])
		n += copied
		r.offset += copied
		if r.offset >= len(r.data) {
			r.offset = 0
		}
	}

	return n, nil
}
