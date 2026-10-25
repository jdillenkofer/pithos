package metadatapart

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

// Readers of external part stores hold no database snapshot once GetObject
// returns. GC retention keeps a part dereferenced during such a GET for at
// least the grace window after GC first observes it unreferenced, so every
// read within leaseFreeWindow of the metadata snapshot is safe without any
// database write. A stream that outlives that window acquires a reader lease
// halfway through it; Condemn skips leased parts for as long as the lease is
// renewed. Streams therefore have no lifetime bound, and only long ones write.
//
// Leases and retention compare timestamps from different instances, so their
// clocks must agree to well within leaseFreeWindow.

var (
	errReaderLeaseCondemned = errors.New("object content was deleted before its reader lease was acquired")
	errReaderLeaseExpired   = errors.New("reader lease expired")
	errReaderLeaseReleased  = errors.New("reader lease released")
)

const readerLeaseReleaseTimeout = 10 * time.Second

type readerLease struct {
	mbs     *metadataPartStorage
	partIds []partstore.PartId
	open    atomic.Int64
	// failure is set once reads are no longer protected; it never clears.
	failure  atomic.Pointer[error]
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// guardReaders protects readers created from a metadata snapshot taken no
// earlier than snapshotAt. ctx must not carry a database transaction.
func (mbs *metadataPartStorage) guardReaders(ctx context.Context, snapshotAt time.Time, partIds []partstore.PartId, readers []io.ReadCloser) {
	l := &readerLease{mbs: mbs, partIds: partIds, stop: make(chan struct{}), done: make(chan struct{})}
	l.open.Store(int64(len(readers)))
	for i, r := range readers {
		readers[i] = &leasedReadCloser{inner: r, lease: l}
	}
	stopOnCancel := context.AfterFunc(ctx, func() { l.fail(ctx.Err()); l.halt() })
	go func() {
		defer stopOnCancel()
		l.run(context.WithoutCancel(ctx), snapshotAt)
	}()
}

func (l *readerLease) err() error {
	if err := l.failure.Load(); err != nil {
		return *err
	}
	return nil
}

func (l *readerLease) fail(err error) {
	l.failure.CompareAndSwap(nil, &err)
}

func (l *readerLease) halt() {
	l.stopOnce.Do(func() { close(l.stop) })
}

// wait returns false once the lease is halted.
func (l *readerLease) wait(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-l.stop:
		return false
	case <-timer.C:
		return true
	}
}

func (l *readerLease) run(ctx context.Context, snapshotAt time.Time) {
	defer close(l.done)
	window := l.mbs.leaseFreeWindow
	if !l.wait(time.Until(snapshotAt.Add(window / 2))) {
		return
	}
	leaseId := ulid.Make().String()
	expiresAt, err := l.acquire(ctx, leaseId, snapshotAt.Add(window))
	if err != nil {
		// Retention still covers the reads so far, but no longer guarantees
		// the content beyond the lease-free window.
		l.fail(err)
		return
	}
	defer l.release(ctx, leaseId)
	ttl := l.mbs.readerLeaseTTL
	for l.wait(ttl / 3) {
		renewed := time.Now().UTC().Add(ttl)
		ok, err := l.renew(ctx, leaseId, expiresAt, renewed)
		switch {
		case err == nil && ok:
			expiresAt = renewed
		case err == nil:
			l.fail(errReaderLeaseExpired)
			return
		case !time.Now().Before(expiresAt):
			l.fail(fmt.Errorf("%w: %w", errReaderLeaseExpired, err))
			return
		default:
			slog.Warn("Renewing reader lease failed; retrying", "lease_id", leaseId, "error", err)
		}
	}
}

// acquire must commit before deadline; retention guarantees until then that
// no part has been condemned, so acquisition only fails for a broken clock or
// a database that stayed unavailable for half the lease-free window.
func (l *readerLease) acquire(ctx context.Context, leaseId string, deadline time.Time) (time.Time, error) {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for {
		expiresAt := time.Now().UTC().Add(l.mbs.readerLeaseTTL)
		var acquired bool
		err := database.WithTx(ctx, l.mbs.db, nil, func(ctx context.Context, tx database.Tx) error {
			var err error
			acquired, err = l.mbs.partRegistryRepository.AcquireReaderLease(ctx, tx.SqlTx(), leaseId, l.partIds, expiresAt)
			return err
		})
		if err == nil && !acquired {
			return time.Time{}, errReaderLeaseCondemned
		}
		if err == nil {
			return expiresAt, nil
		}
		slog.Warn("Acquiring reader lease failed; retrying", "lease_id", leaseId, "error", err)
		select {
		case <-l.stop:
			return time.Time{}, errReaderLeaseReleased
		case <-ctx.Done():
			return time.Time{}, fmt.Errorf("acquiring reader lease: %w", errors.Join(err, ctx.Err()))
		case <-time.After(time.Second):
		}
	}
}

func (l *readerLease) renew(ctx context.Context, leaseId string, expiresAt, renewed time.Time) (bool, error) {
	ctx, cancel := context.WithDeadline(ctx, expiresAt)
	defer cancel()
	var ok bool
	err := database.WithTx(ctx, l.mbs.db, nil, func(ctx context.Context, tx database.Tx) error {
		var err error
		ok, err = l.mbs.partRegistryRepository.RenewReaderLease(ctx, tx.SqlTx(), leaseId, time.Now().UTC(), renewed)
		return err
	})
	return ok, err
}

// release is best effort: GC deletes leases that are not renewed anyway.
func (l *readerLease) release(ctx context.Context, leaseId string) {
	ctx, cancel := context.WithTimeout(ctx, readerLeaseReleaseTimeout)
	defer cancel()
	err := database.WithTx(ctx, l.mbs.db, nil, func(ctx context.Context, tx database.Tx) error {
		return l.mbs.partRegistryRepository.ReleaseReaderLease(ctx, tx.SqlTx(), leaseId)
	})
	if err != nil {
		slog.Warn("Releasing reader lease failed; it expires on its own", "lease_id", leaseId, "error", err)
	}
}

// The last Close waits for the lease to be released, so content is
// collectible again as soon as every reader is closed.
func (l *readerLease) readerClosed() {
	if l.open.Add(-1) == 0 {
		l.fail(errReaderLeaseReleased)
		l.halt()
		<-l.done
	}
}

// leasedReadCloser stops serving content once its lease no longer protects
// it. A Read already blocked in the backend is not interrupted: retention
// still covers it for another leaseFreeWindow after the lease failed.
type leasedReadCloser struct {
	inner     io.ReadCloser
	lease     *readerLease
	closeOnce sync.Once
}

func (r *leasedReadCloser) Read(p []byte) (int, error) {
	if err := r.lease.err(); err != nil {
		return 0, err
	}
	n, err := r.inner.Read(p)
	if leaseErr := r.lease.err(); leaseErr != nil {
		return 0, leaseErr
	}
	return n, err
}

func (r *leasedReadCloser) Close() error {
	err := r.inner.Close()
	r.closeOnce.Do(r.lease.readerClosed)
	return err
}
