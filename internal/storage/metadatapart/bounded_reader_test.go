package metadatapart

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type manualDeadline struct{ context.Context }

func (c manualDeadline) Err() error {
	if c.Context.Err() != nil {
		return context.DeadlineExceeded
	}
	return nil
}
func (c manualDeadline) Deadline() (time.Time, bool) { return time.Now().Add(time.Hour), true }

type blockedReader struct {
	started chan struct{}
	closed  chan struct{}
	release chan struct{}
	once    sync.Once
	reads   atomic.Int64
}

func (r *blockedReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	close(r.started)
	<-r.release
	p[0] = 'x'
	return 1, nil
}
func (r *blockedReader) Close() error { r.once.Do(func() { close(r.closed) }); return nil }

func TestBoundedReaderDeadlineInterruptsReadAndCleansUp(t *testing.T) {
	testutils.SkipIfIntegration(t)
	base, cancel := context.WithCancel(context.Background())
	ctx := manualDeadline{base}
	raw := &blockedReader{started: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
	r, err := openBoundedPart(ctx, func() (io.ReadCloser, error) { return raw, nil })
	require.NoError(t, err)
	result := make(chan error, 1)
	buf := []byte{'a'}
	go func() { _, err := r.Read(buf); result <- err }()
	<-raw.started
	cancel()
	require.ErrorIs(t, <-result, context.DeadlineExceeded)
	select {
	case <-raw.closed:
		t.Fatal("closed stateful reader during Read")
	default:
	}
	require.Equal(t, byte('a'), buf[0])
	_, err = r.Read(buf)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.EqualValues(t, 1, raw.reads.Load())
	close(raw.release)
	<-raw.closed
	require.NoError(t, r.Close())
}

func TestBoundedReaderNoOpenAfterDeadlineAndLateOpenCleanup(t *testing.T) {
	testutils.SkipIfIntegration(t)
	base, cancel := context.WithCancel(context.Background())
	ctx := manualDeadline{base}
	cancel()
	_, err := openBoundedPart(ctx, func() (io.ReadCloser, error) { t.Error("opened after deadline"); return nil, nil })
	require.ErrorIs(t, err, context.DeadlineExceeded)

	base, cancel = context.WithCancel(context.Background())
	ctx = manualDeadline{base}
	started, release := make(chan struct{}), make(chan struct{})
	raw := &blockedReader{closed: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := openBoundedPart(ctx, func() (io.ReadCloser, error) { close(started); <-release; return raw, nil })
		result <- err
	}()
	<-started
	cancel()
	require.ErrorIs(t, <-result, context.DeadlineExceeded)
	close(release)
	<-raw.closed
}

type closeBlockingReader struct {
	started, release chan struct{}
}

func (r *closeBlockingReader) Read([]byte) (int, error) { return 0, io.EOF }
func (r *closeBlockingReader) Close() error {
	close(r.started)
	<-r.release
	return nil
}

func TestBoundedReaderFailedOpenDoesNotWaitForBlockingCleanup(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw := &closeBlockingReader{started: make(chan struct{}), release: make(chan struct{})}
	defer close(raw.release)
	done := make(chan error, 1)
	go func() {
		_, err := openBoundedPart(ctx, func() (io.ReadCloser, error) { return raw, io.ErrUnexpectedEOF })
		done <- err
	}()
	<-raw.started
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	case <-time.After(time.Second):
		t.Fatal("failed open waited for uncooperative Close")
	}
}

func TestSharedDeadlineClosesIdleRanges(t *testing.T) {
	testutils.SkipIfIntegration(t)
	base, cancel := context.WithCancel(context.Background())
	ctx := manualDeadline{base}
	raws := []*blockedReader{{closed: make(chan struct{})}, {closed: make(chan struct{})}}
	readers := make([]io.ReadCloser, 0, 2)
	for _, raw := range raws {
		r, err := openBoundedPart(ctx, func() (io.ReadCloser, error) { return raw, nil })
		require.NoError(t, err)
		readers = append(readers, r)
	}
	cancel()
	for i, r := range readers {
		<-raws[i].closed
		_, err := r.Read(make([]byte, 1))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, raws[i].reads.Load())
		require.NoError(t, r.Close())
	}
}
