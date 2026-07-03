package ioutils

import (
	"errors"
	"io"
)

// readAheadReadSeekCloser wraps a ReadSeekCloser and keeps one block-sized
// fetch in flight ahead of the caller's position. While the caller processes
// block N (e.g. decrypting or writing it to a client), block N+1 is already
// being fetched, hiding the source's round-trip latency. This matters for
// remote sources like SFTP where every read costs at least one WAN round
// trip.
//
// Seek is cheap: SeekStart/SeekCurrent only move the logical position and a
// pending fetch is discarded lazily; sequential consumption after a seek hits
// the prefetched block when positions line up.
//
// The wrapper serializes all access to the inner reader: a fetch goroutine is
// the only user of inner while it runs, and every other operation waits for
// it first. The wrapper itself is not safe for concurrent use, matching the
// io.Reader convention.
type readAheadReadSeekCloser struct {
	inner     io.ReadSeekCloser
	blockSize int

	pos     int64 // caller-visible position
	cur     []byte
	curOff  int64
	curEOF  bool // cur ends at EOF
	pending *pendingFetch
	spare   []byte
	closed  bool
}

type pendingFetch struct {
	off  int64
	done chan struct{}
	buf  []byte
	n    int
	err  error
}

// NewReadAheadReadSeekCloser wraps inner with a single-block read-ahead of
// blockSize bytes. Memory overhead is up to two blocks.
func NewReadAheadReadSeekCloser(inner io.ReadSeekCloser, blockSize int) io.ReadSeekCloser {
	return &readAheadReadSeekCloser{
		inner:     inner,
		blockSize: blockSize,
	}
}

func (r *readAheadReadSeekCloser) startFetch(off int64) {
	buf := r.spare
	if buf == nil {
		buf = make([]byte, r.blockSize)
	}
	r.spare = nil
	p := &pendingFetch{off: off, done: make(chan struct{}), buf: buf}
	r.pending = p
	go func() {
		defer close(p.done)
		if _, err := r.inner.Seek(off, io.SeekStart); err != nil {
			p.err = err
			return
		}
		n, err := io.ReadFull(r.inner, p.buf)
		p.n = n
		if err == io.ErrUnexpectedEOF {
			err = io.EOF
		}
		p.err = err
	}()
}

// waitPending waits for the in-flight fetch (if any) and returns it.
func (r *readAheadReadSeekCloser) waitPending() *pendingFetch {
	p := r.pending
	if p == nil {
		return nil
	}
	r.pending = nil
	<-p.done
	return p
}

func (r *readAheadReadSeekCloser) Read(p []byte) (int, error) {
	if r.closed {
		return 0, errors.New("read on closed reader")
	}
	if len(p) == 0 {
		return 0, nil
	}
	for {
		// Serve from the current block when the position falls inside it.
		if r.cur != nil && r.pos >= r.curOff && r.pos < r.curOff+int64(len(r.cur)) {
			n := copy(p, r.cur[r.pos-r.curOff:])
			r.pos += int64(n)
			return n, nil
		}
		if r.cur != nil && r.curEOF && r.pos >= r.curOff+int64(len(r.cur)) {
			return 0, io.EOF
		}

		// Try to consume the in-flight fetch.
		if pf := r.waitPending(); pf != nil {
			if pf.off == r.pos {
				if pf.err != nil && pf.err != io.EOF {
					r.spare = pf.buf
					return 0, pf.err
				}
				if r.cur != nil && r.spare == nil {
					r.spare = r.cur[:cap(r.cur)]
				}
				r.cur = pf.buf[:pf.n]
				r.curOff = pf.off
				r.curEOF = pf.err == io.EOF
				if pf.n == 0 {
					return 0, io.EOF
				}
				if !r.curEOF {
					r.startFetch(r.curOff + int64(len(r.cur)))
				}
				continue
			}
			// Fetch was for a stale position (caller seeked away); recycle.
			r.spare = pf.buf
		}

		r.startFetch(r.pos)
	}
}

func (r *readAheadReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	if r.closed {
		return 0, errors.New("seek on closed reader")
	}
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		// The stream size is only known to the inner reader; drain any
		// in-flight fetch so we own it, then delegate.
		if pf := r.waitPending(); pf != nil {
			r.spare = pf.buf
		}
		size, err := r.inner.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		abs = size + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("negative position")
	}
	r.pos = abs
	return abs, nil
}

func (r *readAheadReadSeekCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	// The inner reader must not be closed while the fetch goroutine uses it.
	if pf := r.waitPending(); pf != nil {
		r.spare = pf.buf
	}
	return r.inner.Close()
}
