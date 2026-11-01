package metadatapart

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/readerlease"
	"github.com/oklog/ulid/v2"
)

// Readers stream part content after the metadata snapshot that named the
// parts, often without any database transaction. GC retention condemns a
// part no earlier than the grace window after it observed the part
// unreferenced, and that observation follows the dereference, which follows
// the reader's snapshot. Reads within leaseFreeWindow (at most half the
// grace window) of the snapshot are therefore safe without any database
// write.
//
// A reader still open halfway through that window acquires a lease on the
// parts it reads, which must commit before the window ends. GC reads leases
// only after taking its retention cutoff, so it sees every lease that
// matters for a part it may condemn. One loop per instance renews the leases
// it still tracks every TTL/3, in one statement. A reader stops serving content once its lease may
// have expired; a Read that ends within its protection was protected while
// it ran, so a Read that ends later returns an error instead of its data.
//
// Protection deadlines are measured on the local monotonic clock from a
// time no later than the matching database statement, and lease expiry on
// the database clock, so instance clocks only need to agree within the
// grace window minus leaseFreeWindow.

var (
	errReaderLeaseExpired  = errors.New("reader lease expired")
	errReaderLeaseReleased = errors.New("reader lease released")
	errSnapshotTooOld      = errors.New("metadata snapshot is too old to protect its parts")
	errReaderLeasesStopped = errors.New("reader leases stopped")
)

const readerLeaseReleaseTimeout = 10 * time.Second

// maxRevalidationTimeout bounds the fresh read that revalidates a snapshot.
// It needs a second connection while the caller's transaction holds one, so
// an exhausted pool would otherwise block the caller for good.
const maxRevalidationTimeout = 10 * time.Second

// maxLeaseFreeWindow keeps acquisition and renewal frequent enough to
// survive a short database outage with large grace windows.
const maxLeaseFreeWindow = 5 * time.Minute

func leaseFreeWindowFor(graceWindow time.Duration) time.Duration {
	return min(maxLeaseFreeWindow, graceWindow/2)
}

type readerLeases struct {
	store  readerlease.Store
	window time.Duration
	// margin absorbs clock rate differences between the instance and the
	// database.
	margin time.Duration
	epoch  time.Time
	// revalidate, if set, renews a snapshot too old to protect its parts:
	// it returns a time no later than a fresh read that found every part
	// still referenced, or an error.
	revalidate func(ctx context.Context, partIds []partstore.PartId) (time.Time, error)
	// ctx is canceled on shutdown to abort pending acquisitions.
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	acquired map[*readerLease]struct{}
	stopped  bool
	// tasks counts acquisitions and releases that may still use the store.
	tasks sync.WaitGroup
	stop  chan struct{}
	done  chan struct{}
}

func newReaderLeases(store readerlease.Store, window time.Duration) *readerLeases {
	ctx, cancel := context.WithCancel(context.Background())
	return &readerLeases{
		store:    store,
		window:   window,
		margin:   window / 10,
		epoch:    time.Now(),
		ctx:      ctx,
		cancel:   cancel,
		acquired: make(map[*readerLease]struct{}),
	}
}

// monotonic converts t to nanoseconds on the local monotonic clock.
func (rl *readerLeases) monotonic(t time.Time) int64 {
	return t.Sub(rl.epoch).Nanoseconds()
}

func (rl *readerLeases) start() {
	rl.stop = make(chan struct{})
	rl.done = make(chan struct{})
	go rl.renewLoop()
}

// shutdown ends every lease and waits until nothing uses the store anymore.
// Readers still open fail once shutdown begins. The leases are released in
// one statement bounded by ctx and readerLeaseReleaseTimeout; a lease whose
// release fails expires on its own.
func (rl *readerLeases) shutdown(ctx context.Context) {
	rl.mu.Lock()
	if rl.stopped {
		rl.mu.Unlock()
		return
	}
	rl.stopped = true
	leases := make([]*readerLease, 0, len(rl.acquired))
	for lease := range rl.acquired {
		leases = append(leases, lease)
	}
	rl.mu.Unlock()
	rl.cancel()
	if rl.stop != nil {
		close(rl.stop)
		<-rl.done
	}
	// A lease ended concurrently may have skipped its release; releasing
	// twice is harmless.
	leaseIds := make([]string, 0, len(leases))
	for _, lease := range leases {
		lease.finish(errReaderLeasesStopped)
		rl.forget(lease)
		leaseIds = append(leaseIds, lease.id)
	}
	if len(leaseIds) > 0 {
		releaseCtx, cancel := context.WithTimeout(database.ContextWithoutTx(ctx), readerLeaseReleaseTimeout)
		if err := rl.store.Release(releaseCtx, leaseIds); err != nil {
			slog.Warn("Releasing reader leases failed; they expire on their own", "leases", len(leaseIds), "error", err)
		}
		cancel()
	}
	rl.tasks.Wait()
}

// track registers a task that may use the store, unless shutdown began.
// The caller calls rl.tasks.Done when it returns true.
func (rl *readerLeases) track() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.stopped {
		return false
	}
	rl.tasks.Add(1)
	return true
}

func (rl *readerLeases) renewLoop() {
	defer close(rl.done)
	ttl := rl.store.TTL()
	ticker := time.NewTicker(ttl / 3)
	defer ticker.Stop()
	for {
		select {
		case <-rl.stop:
			return
		case <-ticker.C:
		}
		rl.mu.Lock()
		leases := make([]*readerLease, 0, len(rl.acquired))
		leaseIds := make([]string, 0, len(rl.acquired))
		for lease := range rl.acquired {
			leases = append(leases, lease)
			leaseIds = append(leaseIds, lease.id)
		}
		rl.mu.Unlock()
		if len(leases) == 0 {
			continue
		}
		before := time.Now()
		// Shutdown cancels a renewal in flight instead of waiting for it.
		ctx, cancel := context.WithTimeout(rl.ctx, ttl/3)
		err := rl.store.Renew(ctx, leaseIds)
		cancel()
		if err != nil {
			slog.Warn("Renewing reader leases failed; retrying", "leases", len(leases), "error", err)
			continue
		}
		// Only leases still protected after the commit were unexpired when
		// the renewal ran, so only those were extended.
		committed := rl.monotonic(time.Now())
		renewedUntil := rl.monotonic(before.Add(ttl - rl.margin))
		for _, lease := range leases {
			lease.extend(committed, renewedUntil)
		}
	}
}

// begin protects partIds, read from a snapshot taken no earlier than
// snapshotAt (see database.SnapshotTime). The caller holds the lease until
// done; every wrapped reader holds it until closed. Most reads end before
// they need a lease, so begin only arms a timer; the lease id and every
// database write wait for it.
func (rl *readerLeases) begin(ctx context.Context, snapshotAt time.Time, partIds []partstore.PartId) *readerLease {
	if len(partIds) == 0 {
		return nil
	}
	l := &readerLease{
		leases:  rl,
		ctx:     database.ContextWithoutTx(context.WithoutCancel(ctx)),
		partIds: partIds,
		refs:    1,
	}
	deadline := snapshotAt.Add(rl.window - rl.margin)
	if !time.Now().Before(deadline) {
		// A long transaction that reads one snapshot may still name parts
		// that are referenced now; reads of those are as safe as from a
		// fresh snapshot.
		err := errSnapshotTooOld
		if rl.revalidate != nil {
			revalidateCtx, cancel := context.WithTimeout(ctx, min(maxRevalidationTimeout, rl.window/2))
			renewedAt, revalidateErr := rl.revalidate(revalidateCtx, partIds)
			cancel()
			switch {
			case revalidateErr == nil:
				err = nil
				snapshotAt = renewedAt
				deadline = snapshotAt.Add(rl.window - rl.margin)
			case !errors.Is(revalidateErr, errSnapshotTooOld):
				err = fmt.Errorf("%w: revalidating: %w", errSnapshotTooOld, revalidateErr)
			}
		}
		if err != nil {
			l.protectedUntil.Store(rl.monotonic(deadline))
			l.fail(err)
			return l
		}
	}
	l.protectedUntil.Store(rl.monotonic(deadline))
	// The lease ends with the request even if a reader is never closed, so
	// the instance does not renew it forever. AfterFunc runs end at once for
	// a canceled ctx; holding mu makes it wait until both fields are set.
	// Shutdown ends it too, including one still within the lease-free window.
	l.mu.Lock()
	l.stopOnCancel = context.AfterFunc(ctx, func() { l.end(ctx.Err()) })
	l.stopOnShutdown = context.AfterFunc(rl.ctx, func() { l.end(errReaderLeasesStopped) })
	l.timer = time.AfterFunc(time.Until(snapshotAt.Add(rl.window/2)), func() {
		if !rl.track() {
			l.fail(errReaderLeasesStopped)
			return
		}
		defer rl.tasks.Done()
		l.acquire(deadline)
	})
	l.mu.Unlock()
	return l
}

type readerLease struct {
	leases *readerLeases
	ctx    context.Context
	// id is set when the acquisition starts.
	id      string
	partIds []partstore.PartId

	// protectedUntil is the local monotonic deadline of the protection.
	protectedUntil atomic.Int64
	// failure is set once reads are no longer protected; it never clears.
	failure atomic.Pointer[error]

	mu             sync.Mutex
	refs           int
	timer          *time.Timer
	stopOnCancel   func() bool
	stopOnShutdown func() bool
	acquired       bool
	ended          bool
}

func (l *readerLease) err() error {
	if err := l.failure.Load(); err != nil {
		return *err
	}
	// The part stores stop after shutdown, possibly before stopOnShutdown
	// ran.
	if l.leases.ctx.Err() != nil {
		return errReaderLeasesStopped
	}
	if l.leases.monotonic(time.Now()) >= l.protectedUntil.Load() {
		return errReaderLeaseExpired
	}
	return nil
}

func (l *readerLease) fail(err error) {
	l.failure.CompareAndSwap(nil, &err)
}

func (l *readerLease) extend(committed, renewedUntil int64) {
	for {
		current := l.protectedUntil.Load()
		if current <= committed || current >= renewedUntil {
			return
		}
		if l.protectedUntil.CompareAndSwap(current, renewedUntil) {
			return
		}
	}
}

func (l *readerLease) isEnded() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ended
}

func (l *readerLease) acquire(deadline time.Time) {
	if l.isEnded() {
		return
	}
	l.id = ulid.Make().String()
	ctx, cancel := context.WithDeadline(l.ctx, deadline)
	defer cancel()
	defer context.AfterFunc(l.leases.ctx, cancel)()
	for {
		before := time.Now()
		err := l.leases.store.Acquire(ctx, l.id, l.partIds)
		if err == nil {
			// A lease committed after the protection lapsed may be too late
			// for GC, so it must not revive the reader.
			if l.leases.monotonic(time.Now()) >= l.protectedUntil.Load() {
				l.fail(errReaderLeaseExpired)
				l.release()
				return
			}
			l.protectedUntil.Store(l.leases.monotonic(before.Add(l.leases.store.TTL() - l.leases.margin)))
			break
		}
		if l.isEnded() {
			return
		}
		slog.Warn("Acquiring reader lease failed; retrying", "lease_id", l.id, "error", err)
		select {
		case <-ctx.Done():
			// Retention still covers the reads until the deadline, but not
			// beyond it.
			l.fail(fmt.Errorf("acquiring reader lease: %w", errors.Join(err, ctx.Err())))
			return
		case <-time.After(time.Second):
		}
	}

	l.mu.Lock()
	l.acquired = true
	ended := l.ended
	if !ended {
		l.leases.mu.Lock()
		// shutdown ends only the leases it finds here.
		if l.leases.stopped {
			ended = true
		} else {
			l.leases.acquired[l] = struct{}{}
		}
		l.leases.mu.Unlock()
	}
	l.mu.Unlock()
	if ended {
		l.fail(errReaderLeasesStopped)
		l.release()
	}
}

// release is best effort: a lease that is no longer renewed expires, and GC
// deletes it.
func (l *readerLease) release() {
	l.leases.forget(l)
	ctx, cancel := context.WithTimeout(l.ctx, readerLeaseReleaseTimeout)
	defer cancel()
	if err := l.leases.store.Release(ctx, []string{l.id}); err != nil {
		slog.Warn("Releasing reader lease failed; it expires on its own", "lease_id", l.id, "error", err)
	}
}

func (rl *readerLeases) forget(l *readerLease) {
	rl.mu.Lock()
	delete(rl.acquired, l)
	rl.mu.Unlock()
}

// end stops protecting the parts. A lease still being acquired is released
// by the acquisition once it commits. The release runs in the background so
// that closing a reader does not wait for the database.
func (l *readerLease) end(err error) {
	if !l.finish(err) {
		return
	}
	l.leases.forget(l)
	// After shutdown began, shutdown releases the lease.
	if !l.leases.track() {
		return
	}
	go func() {
		defer l.leases.tasks.Done()
		l.release()
	}()
}

// finish marks the lease ended and reports whether the caller ended an
// acquired lease, which it then has to release.
func (l *readerLease) finish(err error) bool {
	l.fail(err)
	l.mu.Lock()
	if l.ended {
		l.mu.Unlock()
		return false
	}
	l.ended = true
	if l.timer != nil {
		l.timer.Stop()
	}
	if l.stopOnCancel != nil {
		l.stopOnCancel()
	}
	if l.stopOnShutdown != nil {
		l.stopOnShutdown()
	}
	acquired := l.acquired
	l.mu.Unlock()
	return acquired
}

func (l *readerLease) hold() {
	l.mu.Lock()
	l.refs++
	l.mu.Unlock()
}

func (l *readerLease) done() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.refs--
	last := l.refs == 0
	l.mu.Unlock()
	if last {
		l.end(errReaderLeaseReleased)
	}
}

// wrap makes rc fail once the lease no longer protects its parts. The
// reader holds the lease until it is closed.
func (l *readerLease) wrap(rc io.ReadCloser) io.ReadCloser {
	if l == nil {
		return rc
	}
	l.hold()
	return &leasedReadCloser{inner: rc, lease: l}
}

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
	r.closeOnce.Do(r.lease.done)
	return err
}

// revalidateSnapshot returns a time no later than a fresh read that found
// every part in partIds referenced. A referenced part has not started its
// retention, so that time protects reads like a snapshot taken then.
func (mbs *metadataPartStorage) revalidateSnapshot(ctx context.Context, partIds []partstore.PartId) (time.Time, error) {
	renewedAt := time.Now()
	err := database.WithTx(database.ContextWithoutTx(ctx), mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		for _, partId := range partIds {
			referenced, err := mbs.partRegistry.IsReferenced(ctx, tx.SqlTx(), partId)
			if err != nil {
				return err
			}
			if !referenced {
				return errSnapshotTooOld
			}
		}
		return nil
	})
	return renewedAt, err
}

// crossStorePartIds returns the ids of parts outside storeName, which a copy
// into storeName has to read.
func crossStorePartIds(parts []metadatastore.Part, storeName *string) []partstore.PartId {
	var partIds []partstore.PartId
	for _, part := range parts {
		if !partStoreNamesEqual(part.StoreName, storeName) {
			partIds = append(partIds, part.Id)
		}
	}
	return partIds
}
