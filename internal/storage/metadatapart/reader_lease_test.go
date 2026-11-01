package metadatapart

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/readerlease"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

const testLeaseFreeWindow = 300 * time.Millisecond

type faultyLeaseStore struct {
	readerlease.Store
	acquires    atomic.Int64
	failAcquire atomic.Bool
	failRenew   atomic.Bool
	// blockRenew makes renewals signal renewStarted, wait for their context
	// and report why it ended on renewEnded.
	blockRenew    atomic.Bool
	renewStarted  chan struct{}
	renewEnded    chan error
	failRelease   atomic.Bool
	releases      atomic.Int64
	renewedLeases atomic.Int64
	// acquireDelay delays acquisitions, which then commit regardless of
	// their deadline.
	acquireDelay atomic.Int64
}

func (s *faultyLeaseStore) Acquire(ctx context.Context, leaseId string, partIds []partstore.PartId) error {
	s.acquires.Add(1)
	if s.failAcquire.Load() {
		return errors.New("database unavailable")
	}
	if delay := time.Duration(s.acquireDelay.Load()); delay > 0 {
		time.Sleep(delay)
		ctx = context.WithoutCancel(ctx)
	}
	return s.Store.Acquire(ctx, leaseId, partIds)
}

func (s *faultyLeaseStore) Renew(ctx context.Context, leaseIds []string) error {
	if s.failRenew.Load() {
		return errors.New("database unavailable")
	}
	if s.blockRenew.Load() {
		select {
		case s.renewStarted <- struct{}{}:
		default:
		}
		<-ctx.Done()
		select {
		case s.renewEnded <- ctx.Err():
		default:
		}
		return ctx.Err()
	}
	s.renewedLeases.Store(int64(len(leaseIds)))
	return s.Store.Renew(ctx, leaseIds)
}

func (s *faultyLeaseStore) Release(ctx context.Context, leaseIds []string) error {
	s.releases.Add(1)
	if s.failRelease.Load() {
		return errors.New("database unavailable")
	}
	return s.Store.Release(ctx, leaseIds)
}

func newTestReaderLeases(t *testing.T) (*readerLeases, *faultyLeaseStore) {
	t.Helper()
	store := &faultyLeaseStore{Store: readerlease.NewMemoryStore(testLeaseFreeWindow), renewStarted: make(chan struct{}, 1), renewEnded: make(chan error, 1)}
	leases := newReaderLeases(store, testLeaseFreeWindow)
	leases.start()
	t.Cleanup(func() { leases.shutdown(context.Background()) })
	return leases, store
}

func newLeasedReader(t *testing.T, leases *readerLeases, ctx context.Context, snapshotAt time.Time) (io.ReadCloser, partstore.PartId) {
	t.Helper()
	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	lease := leases.begin(ctx, snapshotAt, []partstore.PartId{*partId})
	reader := lease.wrap(io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), 1024))))
	lease.done()
	return reader, *partId
}

func isLeased(t *testing.T, store readerlease.Store, partId partstore.PartId) bool {
	t.Helper()
	leased, err := store.Leased(context.Background(), nil, []partstore.PartId{partId})
	require.NoError(t, err)
	_, ok := leased[partId]
	return ok
}

func TestShortReadsTakeNoReaderLease(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	reader, _ := newLeasedReader(t, leases, context.Background(), time.Now())
	_, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	time.Sleep(testLeaseFreeWindow)
	require.Zero(t, store.acquires.Load())
}

func TestLongReadsHoldRenewedReaderLeaseUntilClosed(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	reader, partId := newLeasedReader(t, leases, context.Background(), time.Now())
	require.Eventually(t, func() bool { return isLeased(t, store, partId) }, testLeaseFreeWindow, 10*time.Millisecond)
	// Several TTLs pass; only renewal keeps the reader protected.
	time.Sleep(3 * testLeaseFreeWindow)
	require.True(t, isLeased(t, store, partId))
	_, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Eventually(t, func() bool { return !isLeased(t, store, partId) }, time.Second, 10*time.Millisecond)
}

func TestReadsFailWhenReaderLeaseCannotBeAcquired(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	store.failAcquire.Store(true)
	reader, _ := newLeasedReader(t, leases, context.Background(), time.Now())
	defer reader.Close()
	time.Sleep(testLeaseFreeWindow)
	_, err := reader.Read(make([]byte, 1))
	require.Error(t, err)
	require.NotZero(t, store.acquires.Load())
}

func TestReadsFailWhenReaderLeaseIsNotRenewed(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	reader, partId := newLeasedReader(t, leases, context.Background(), time.Now())
	defer reader.Close()
	require.Eventually(t, func() bool { return isLeased(t, store, partId) }, testLeaseFreeWindow, 10*time.Millisecond)
	store.failRenew.Store(true)
	time.Sleep(testLeaseFreeWindow + 50*time.Millisecond)
	_, err := reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, errReaderLeaseExpired)
}

func TestRequestEndReleasesReaderLease(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	ctx, cancel := context.WithCancel(context.Background())
	reader, partId := newLeasedReader(t, leases, ctx, time.Now())
	defer reader.Close()
	require.Eventually(t, func() bool { return isLeased(t, store, partId) }, testLeaseFreeWindow, 10*time.Millisecond)
	cancel()
	require.Eventually(t, func() bool { return !isLeased(t, store, partId) }, time.Second, 10*time.Millisecond)
	_, err := reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, context.Canceled)
}

func TestReadsFromTooOldSnapshotFail(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	reader, _ := newLeasedReader(t, leases, context.Background(), time.Now().Add(-testLeaseFreeWindow))
	defer reader.Close()
	_, err := reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, errSnapshotTooOld)
	require.Zero(t, store.acquires.Load())
}

func TestReadsFromRevalidatedOldSnapshotSucceed(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	leases.revalidate = func(ctx context.Context, partIds []partstore.PartId) (time.Time, error) {
		return time.Now(), nil
	}
	reader, partId := newLeasedReader(t, leases, context.Background(), time.Now().Add(-testLeaseFreeWindow))
	defer reader.Close()
	require.Eventually(t, func() bool { return isLeased(t, store, partId) }, testLeaseFreeWindow, 10*time.Millisecond)
	_, err := io.ReadAll(reader)
	require.NoError(t, err)
}

func TestReaderLeaseWhoseReleaseFailedIsNotRenewed(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	orphaned, orphanedPartId := newLeasedReader(t, leases, context.Background(), time.Now())
	active, activePartId := newLeasedReader(t, leases, context.Background(), time.Now())
	defer active.Close()
	require.Eventually(t, func() bool {
		return isLeased(t, store, orphanedPartId) && isLeased(t, store, activePartId)
	}, testLeaseFreeWindow, 10*time.Millisecond)
	store.failRelease.Store(true)
	require.NoError(t, orphaned.Close())
	require.Eventually(t, func() bool { return store.releases.Load() > 0 }, time.Second, 10*time.Millisecond)
	store.failRelease.Store(false)
	// The active lease keeps the renewal loop busy; the orphaned one expires.
	require.Eventually(t, func() bool { return !isLeased(t, store, orphanedPartId) }, 2*testLeaseFreeWindow, 10*time.Millisecond)
	require.True(t, isLeased(t, store, activePartId))
	require.Equal(t, int64(1), store.renewedLeases.Load())
}

func TestReaderLeaseOfCanceledRequestEndsCleanly(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader, _ := newLeasedReader(t, leases, ctx, time.Now())
	defer reader.Close()
	time.Sleep(testLeaseFreeWindow)
	require.Zero(t, store.acquires.Load())
	_, err := reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, context.Canceled)
}

func TestLateReaderLeaseAcquisitionDoesNotReviveProtection(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	// The acquisition commits only after the protection lapsed.
	store.acquireDelay.Store(int64(testLeaseFreeWindow))
	reader, partId := newLeasedReader(t, leases, context.Background(), time.Now())
	defer reader.Close()
	require.Eventually(t, func() bool { return store.acquires.Load() > 0 }, testLeaseFreeWindow, 10*time.Millisecond)
	time.Sleep(2 * testLeaseFreeWindow)
	require.False(t, isLeased(t, store, partId))
	for range 2 {
		_, err := reader.Read(make([]byte, 1))
		require.ErrorIs(t, err, errReaderLeaseExpired)
		time.Sleep(testLeaseFreeWindow / 2)
	}
	require.False(t, isLeased(t, store, partId))
}

func TestShutdownReleasesReaderLeases(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	reader, partId := newLeasedReader(t, leases, context.Background(), time.Now())
	defer reader.Close()
	other, otherPartId := newLeasedReader(t, leases, context.Background(), time.Now())
	defer other.Close()
	require.Eventually(t, func() bool {
		return isLeased(t, store, partId) && isLeased(t, store, otherPartId)
	}, testLeaseFreeWindow, 10*time.Millisecond)
	// Both leases end in one release.
	leases.shutdown(context.Background())
	require.False(t, isLeased(t, store, partId))
	require.Equal(t, int64(1), store.releases.Load())
	_, err := reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, errReaderLeasesStopped)
}

func TestShutdownCancelsRenewalInFlight(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	reader, partId := newLeasedReader(t, leases, context.Background(), time.Now())
	defer reader.Close()
	require.Eventually(t, func() bool { return isLeased(t, store, partId) }, testLeaseFreeWindow, 10*time.Millisecond)
	// The renewal waits like one blocked on an exhausted connection pool.
	store.blockRenew.Store(true)
	select {
	case <-store.renewStarted:
	case <-time.After(time.Second):
		t.Fatal("no renewal started")
	}
	leases.shutdown(context.Background())
	require.ErrorIs(t, <-store.renewEnded, context.Canceled)
}

func TestShutdownStopsPendingReaderLeaseAcquisitions(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	store.failAcquire.Store(true)
	retrying, _ := newLeasedReader(t, leases, context.Background(), time.Now())
	defer retrying.Close()
	require.Eventually(t, func() bool { return store.acquires.Load() > 0 }, testLeaseFreeWindow, 10*time.Millisecond)
	pending, _ := newLeasedReader(t, leases, context.Background(), time.Now())
	defer pending.Close()
	leases.shutdown(context.Background())
	acquires := store.acquires.Load()
	time.Sleep(testLeaseFreeWindow)
	require.Equal(t, acquires, store.acquires.Load())
	_, err := pending.Read(make([]byte, 1))
	require.ErrorIs(t, err, errReaderLeasesStopped)
	_, err = retrying.Read(make([]byte, 1))
	require.Error(t, err)
}

func TestShutdownStopsReadersWithinLeaseFreeWindow(t *testing.T) {
	testutils.SkipIfIntegration(t)
	leases, store := newTestReaderLeases(t)
	reader, _ := newLeasedReader(t, leases, context.Background(), time.Now())
	defer reader.Close()
	leases.shutdown(context.Background())
	// The part stores stop right after shutdown, so the reader must not wait
	// for its acquisition timer to learn about it.
	_, err := reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, errReaderLeasesStopped)
	time.Sleep(testLeaseFreeWindow)
	require.Equal(t, int64(0), store.acquires.Load())
}

func TestTooShortGCGraceWindowIsRejected(t *testing.T) {
	testutils.SkipIfIntegration(t)
	var options storageOptions
	require.Error(t, WithGCGraceWindow(5*time.Nanosecond)(&options))
	require.NoError(t, WithGCGraceWindow(minGCGraceWindow)(&options))
}
