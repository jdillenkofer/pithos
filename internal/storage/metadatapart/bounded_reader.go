package metadatapart

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
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
				r.rc.Close()
			}
			return nil, err
		}
		if r.err != nil {
			if r.rc != nil {
				r.rc.Close()
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
	once   sync.Once
	closed atomic.Bool
	stop   func() bool
}

func (b *boundedReader) closeInner()  { b.once.Do(func() { b.closed.Store(true); _ = b.inner.Close() }) }
func (b *boundedReader) Close() error { b.stop(); b.closeInner(); return nil }

func (b *boundedReader) Read(p []byte) (int, error) {
	if b.closed.Load() && b.ctx.Err() == nil {
		return 0, io.EOF
	}
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	// Never let a late backend read mutate the caller's buffer after returning.
	buf := make([]byte, len(p))
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		if err := b.ctx.Err(); err != nil {
			done <- result{0, err}
			return
		}
		n, err := b.inner.Read(buf)
		done <- result{n, err}
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
