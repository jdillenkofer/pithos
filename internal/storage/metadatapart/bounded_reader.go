package metadatapart

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

// openBoundedPart also handles backends whose open does not honor context.
// Late results are closed, never exposed to the caller.
func openBoundedPart(ctx context.Context, open func() (io.ReadCloser, error)) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		rc  io.ReadCloser
		err error
	}
	done := make(chan result)
	go func() {
		if ctx.Err() != nil {
			return
		}
		rc, err := open()
		select {
		case done <- result{rc, err}:
		case <-ctx.Done():
			if rc != nil {
				rc.Close()
			}
		}
	}()
	select {
	case r := <-done:
		if err := ctx.Err(); err != nil {
			if r.rc != nil {
				go r.rc.Close()
			}
			return nil, err
		}
		if r.err != nil {
			if r.rc != nil {
				go r.rc.Close()
			}
			return nil, r.err
		}
		b := &boundedReader{ctx: ctx, inner: r.rc}
		b.stop = context.AfterFunc(ctx, func() { b.closeInner() })
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type lifetimeReader struct {
	io.ReadCloser
	deadline time.Time
}

func (r *lifetimeReader) Deadline() time.Time { return r.deadline }

type boundedReader struct {
	ctx    context.Context
	inner  io.ReadCloser
	mu     sync.Mutex
	busy   bool
	closed bool
	stop   func() bool
}

// Mark closed immediately, but never release decoder/buffer state during Read.
// The outstanding operation owns cleanup if it cannot be interrupted safely.
func (b *boundedReader) closeInner() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	closeNow := !b.busy
	b.mu.Unlock()
	if closeNow {
		_ = b.inner.Close()
	}
}
func (b *boundedReader) Close() error { b.stop(); b.closeInner(); return nil }

func (b *boundedReader) Read(p []byte) (int, error) {
	b.mu.Lock()
	if err := b.ctx.Err(); err != nil {
		b.mu.Unlock()
		return 0, err
	}
	if b.closed {
		b.mu.Unlock()
		return 0, io.EOF
	}
	if b.busy {
		b.mu.Unlock()
		return 0, fmt.Errorf("concurrent bounded reader Read")
	}
	b.busy = true
	b.mu.Unlock()
	// Never let a late backend read mutate the caller's buffer after returning.
	buf := make([]byte, len(p))
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		var r result
		if err := b.ctx.Err(); err != nil {
			r.err = err
		} else {
			r.n, r.err = b.inner.Read(buf)
		}
		b.mu.Lock()
		b.busy = false
		closeNow := b.closed
		b.mu.Unlock()
		if closeNow {
			_ = b.inner.Close()
		}
		done <- r
	}()
	select {
	case r := <-done:
		if err := b.ctx.Err(); err != nil {
			return 0, err
		}
		copy(p, buf[:r.n])
		return r.n, r.err
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	}
}
