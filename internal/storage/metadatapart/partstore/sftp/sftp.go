package sftp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

const maxStpRetries = 5

// readAheadBlockSize is the prefetch block size for part downloads. While a
// consumer processes one block (decrypting, writing to the client), the next
// block is already in flight, hiding the WAN round trip. pkg/sftp splits each
// block read into concurrent 32KB requests, so this also controls download
// parallelism. Memory cost is up to two blocks per open part reader.
const readAheadBlockSize = 1024 * 1024

// defaultConnectionPoolSize is the number of SSH connections per store.
// Servers often limit concurrent connections per user, so more than one
// is opt-in.
const defaultConnectionPoolSize = 1

var errNotConnected = errors.New("sftp part store is not connected")

type sftpPartStore struct {
	addr         string
	clientConfig *ssh.ClientConfig
	root         string
	slots        []*connSlot
	// next rotates the starting point of the least-busy search, so idle
	// connections take turns.
	next   atomic.Uint32
	tracer trace.Tracer
}

// connSlot is one connection of the pool. Its connection is replaced as a
// whole on reconnect, so every request sees a consistent SSH connection and
// SFTP client.
type connSlot struct {
	conn atomic.Pointer[sftpConn]
	// inUse counts leases, including those still on a replaced connection.
	inUse atomic.Int32
	// reconnectMu serializes reconnects of this slot.
	reconnectMu sync.Mutex
}

type sftpConn struct {
	sshClient *ssh.Client
	client    *sftp.Client
}

func (c *sftpConn) close() error {
	_ = c.client.Close()
	return c.sshClient.Close()
}

// clientLease is a connection taken from the pool. Files opened through it
// must be closed before the lease is released.
type clientLease struct {
	slot *connSlot
	conn *sftpConn
}

func (l *clientLease) release() {
	l.slot.inUse.Add(-1)
}

// acquire leases the least busy connection. The lease has no connection if
// the slot is not connected, so the caller can reconnect it.
func (s *sftpPartStore) acquire() *clientLease {
	n := len(s.slots)
	start := int(s.next.Add(1)) % n
	best := s.slots[start]
	for i := 1; i < n; i++ {
		slot := s.slots[(start+i)%n]
		if slot.inUse.Load() < best.inUse.Load() {
			best = slot
		}
	}
	best.inUse.Add(1)
	return &clientLease{slot: best, conn: best.conn.Load()}
}

// withClient runs op on a leased client without retrying.
func (s *sftpPartStore) withClient(op func(*sftp.Client) error) error {
	lease := s.acquire()
	defer lease.release()
	if lease.conn == nil {
		return errNotConnected
	}
	return op(lease.conn.client)
}

// doRetriableWithClient runs op on a leased client and retries failures on
// a fresh connection. On success the caller owns the returned lease and
// must release it.
func doRetriableWithClient[T any](ctx context.Context, s *sftpPartStore, op func(*sftp.Client) (T, error), shouldIgnoreError func(error) bool) (T, *clientLease, error) {
	// failed is the lease of the last failed attempt.
	var failed *clientLease
	var lease *clientLease
	t, err := doRetriableOperation(ctx, func() (T, error) {
		var empty T
		l := s.acquire()
		if l.conn == nil {
			l.release()
			failed = l
			return empty, errNotConnected
		}
		t, err := op(l.conn.client)
		if err != nil {
			l.release()
			failed = l
			return empty, err
		}
		lease = l
		return t, nil
	}, maxStpRetries, func() error {
		return s.reconnect(failed.slot, failed.conn)
	}, shouldIgnoreError)
	if err != nil {
		return t, nil, err
	}
	return t, lease, nil
}

// Compile-time check to ensure sftpPartStore implements partstore.PartStore
var _ partstore.PartStore = (*sftpPartStore)(nil)
var _ partstore.StatsProvider = (*sftpPartStore)(nil)

func (s *sftpPartStore) SupportsStats() bool { return true }

func (s *sftpPartStore) ensureRootDir() error {
	_, lease, err := doRetriableWithClient(context.Background(), s, func(client *sftp.Client) (*struct{}, error) {
		return nil, client.MkdirAll(s.root)
	}, nil)
	if err != nil {
		return err
	}
	lease.release()
	return nil
}

func (s *sftpPartStore) getFilename(partId partstore.PartId) string {
	partFilename := hex.EncodeToString(partId.Bytes())
	return filepath.Join(s.root, partFilename)
}

func (s *sftpPartStore) tryGetPartIdFromFilename(filename string) (partId *partstore.PartId, ok bool) {
	if len(filename) != 32 {
		return nil, false
	}
	partIdBytes, err := hex.DecodeString(filename)
	if err != nil {
		return nil, false
	}
	partId, err = partstore.NewPartIdFromBytes(partIdBytes)
	if err != nil {
		return nil, false
	}
	return partId, true
}

// reconnect replaces the connection failed of slot with a new one. If
// another goroutine already replaced it, it returns without dialing, so
// concurrent failures of one connection cause a single reconnect. Requests
// still using the old connection fail and retry on the new one.
func (s *sftpPartStore) reconnect(slot *connSlot, failed *sftpConn) error {
	slot.reconnectMu.Lock()
	defer slot.reconnectMu.Unlock()

	if slot.conn.Load() != failed {
		return nil
	}
	if failed != nil {
		slog.Info(fmt.Sprintf("SFTP reconnecting to %s", s.addr))
		_ = failed.close()
	}

	conn, err := s.dial()
	if err != nil {
		return err
	}
	slot.conn.Store(conn)
	slog.Info(fmt.Sprintf("SFTP successfully reconnected to %s", s.addr))
	return nil
}

func (s *sftpPartStore) dial() (*sftpConn, error) {
	sshClient, err := ssh.Dial("tcp", s.addr, s.clientConfig)
	if err != nil {
		slog.Error(fmt.Sprintf("SFTP failed to establish SSH connection to %s: %v", s.addr, err))
		return nil, err
	}
	client, err := sftp.NewClient(sshClient)
	if err != nil {
		slog.Error(fmt.Sprintf("SFTP failed to create SFTP client for %s: %v", s.addr, err))
		_ = sshClient.Close()
		return nil, err
	}
	return &sftpConn{sshClient: sshClient, client: client}, nil
}

// getBackoffDuration returns the backoff duration for a given retry attempt.
// First retry is immediate, subsequent retries use exponential backoff:
// attempt 1: 0ms, attempt 2: 100ms, attempt 3: 1s, attempt 4: 5s, attempt 5: 10s
func getBackoffDuration(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 0 // First retry is immediate
	case 2:
		return 100 * time.Millisecond
	case 3:
		return 1 * time.Second
	case 4:
		return 5 * time.Second
	default:
		return 10 * time.Second
	}
}

func doRetriableOperation[T any](ctx context.Context, op func() (T, error), maxRetries int, preRetry func() error, shouldIgnoreError func(error) bool) (T, error) {
	retries := 0
	var empty T
	for {
		if err := ctx.Err(); err != nil {
			return empty, err
		}

		t, err := op()
		if err != nil {
			if shouldIgnoreError != nil && shouldIgnoreError(err) {
				return empty, err
			}

			retries += 1
			if retries < maxRetries {
				backoff := getBackoffDuration(retries)
				if backoff > 0 {
					slog.Warn(fmt.Sprintf("SFTP operation failed (attempt %d/%d): %v, waiting %v before reconnect", retries, maxRetries, err, backoff))
					select {
					case <-ctx.Done():
						return empty, ctx.Err()
					case <-time.After(backoff):
					}
				} else {
					slog.Warn(fmt.Sprintf("SFTP operation failed (attempt %d/%d): %v, attempting immediate reconnect", retries, maxRetries, err))
				}
				if err := ctx.Err(); err != nil {
					return empty, err
				}
				err = preRetry()
				if err != nil {
					slog.Error(fmt.Sprintf("SFTP reconnect failed: %v", err))
					return empty, err
				}
				continue
			}
			slog.Error(fmt.Sprintf("SFTP operation failed after %d retries: %v", retries, err))
			return empty, err
		}
		return t, nil
	}
}

type Option func(*sftpPartStore)

// WithConnectionPoolSize sets the number of SSH connections. Requests use
// the least busy one. A single connection carries at most one TCP receive
// window per round trip, x/crypto/ssh caps each SSH channel at 2MB per
// round trip, and the server runs one sftp-server process per connection.
// More connections speed up concurrent transfers when the link is not the
// bottleneck, e.g. a fast local server or a fast high-latency link, but
// not on links slower than one connection already fills.
func WithConnectionPoolSize(connectionPoolSize int) Option {
	return func(s *sftpPartStore) {
		if connectionPoolSize > 0 {
			s.slots = make([]*connSlot, connectionPoolSize)
		}
	}
}

func New(addr string, clientConfig *ssh.ClientConfig, root string, opts ...Option) (partstore.PartStore, error) {
	bs := &sftpPartStore{
		addr:         addr,
		clientConfig: clientConfig,
		root:         root,
		slots:        make([]*connSlot, defaultConnectionPoolSize),
		tracer:       otel.Tracer("internal/storage/metadatapart/partstore/sftp"),
	}
	for _, opt := range opts {
		opt(bs)
	}
	for i := range bs.slots {
		bs.slots[i] = &connSlot{}
	}
	return bs, nil
}

func (s *sftpPartStore) Start(ctx context.Context) error {
	// Each connection costs an SSH handshake, so dial them concurrently.
	errs := make([]error, len(s.slots))
	var wg sync.WaitGroup
	for i, slot := range s.slots {
		wg.Go(func() {
			errs[i] = s.reconnect(slot, slot.conn.Load())
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		_ = s.Stop(ctx)
		return err
	}
	if err := s.ensureRootDir(); err != nil {
		_ = s.Stop(ctx)
		return err
	}
	if err := s.cleanupTemporaryParts(); err != nil {
		_ = s.Stop(ctx)
		return err
	}
	return nil
}

func (s *sftpPartStore) cleanupTemporaryParts() error {
	return s.withClient(func(client *sftp.Client) error {
		entries, err := client.ReadDir(s.root)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !entry.Mode().IsRegular() || !partstore.IsTemporaryPartFilename(entry.Name()) {
				continue
			}
			if err := client.Remove(filepath.Join(s.root, entry.Name())); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *sftpPartStore) Stop(ctx context.Context) error {
	var errs []error
	for _, slot := range s.slots {
		slot.reconnectMu.Lock()
		if conn := slot.conn.Swap(nil); conn != nil {
			errs = append(errs, conn.close())
		}
		slot.reconnectMu.Unlock()
	}
	return errors.Join(errs...)
}

func (s *sftpPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	_, span := s.tracer.Start(ctx, "sftpPartStore.PutPart")
	defer span.End()

	filename := s.getFilename(partId)
	if tx != nil {
		// Part ids are fresh random ids, so the part usually does not exist
		// yet. Nothing can reference it before commit, so write it in place
		// and remove it on rollback. SFTPv3 servers report an existing file
		// as a generic failure, so status errors fall back to a temporary file.
		// Existing parts are replaced after the temporary upload completes.
		f, lease, err := doRetriableWithClient(ctx, s, func(client *sftp.Client) (*sftp.File, error) {
			return client.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY)
		}, isStatusError)
		if err == nil {
			err := writeAndClose(lease.conn.client, f, reader)
			if err != nil {
				_ = lease.conn.client.Remove(filename)
			}
			lease.release()
			if err != nil {
				return err
			}
			tx.OnRollback(func(context.Context) error {
				return s.withClient(func(client *sftp.Client) error {
					if err := client.Remove(filename); err != nil && !isNotFoundError(err) {
						return err
					}
					return nil
				})
			})
			return nil
		}
		if !isStatusError(err) {
			return err
		}
	}

	// The ID represents immutable logical content. A retry or repair can safely
	// publish before commit, but must not remove a pre-existing part on rollback
	// or truncate the previous copy in place.
	tempName := filepath.Join(s.root, partstore.NewTemporaryPartFilename(partId))
	f, lease, err := doRetriableWithClient(ctx, s, func(client *sftp.Client) (*sftp.File, error) {
		// This name is private to this invocation, and uploading starts only
		// after open succeeds. Truncation makes a lost open response retryable.
		return client.OpenFile(tempName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY)
	}, nil)
	if err != nil {
		return err
	}
	defer lease.release()
	client := lease.conn.client
	defer client.Remove(tempName)
	if err := writeAndClose(client, f, reader); err != nil {
		return err
	}
	return publishTemporaryPart(client, tempName, filename)
}

type partPublisher interface {
	HasExtension(string) (string, bool)
	PosixRename(string, string) error
	Rename(string, string) error
	Remove(string) error
}

func publishTemporaryPart(client partPublisher, tempName, filename string) error {
	if _, ok := client.HasExtension("posix-rename@openssh.com"); ok {
		return client.PosixRename(tempName, filename)
	}
	// Standard SFTPv3 rename cannot overwrite a destination. Remove it only
	// after the upload completes. This fallback is not atomic: a crash or
	// rename failure after removal can leave the final part missing.
	if err := client.Remove(filename); err != nil && !isNotFoundError(err) {
		return err
	}
	return client.Rename(tempName, filename)
}

// writeAndClose copies reader into f, syncs it when advertised, and closes it. File.ReadFrom only
// pipelines writes for readers with a known size and otherwise waits a full
// round trip per 32KB packet, so request the concurrent upload explicitly.
// A failed concurrent upload can leave holes, so callers must remove the
// file on error.
func writeAndClose(client *sftp.Client, f *sftp.File, reader io.Reader) error {
	_, err := f.ReadFromWithConcurrency(reader, 0)
	if _, supported := client.HasExtension("fsync@openssh.com"); err == nil && supported {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

func (s *sftpPartStore) Capabilities() partstore.Capabilities {
	return partstore.NewCapabilities(
		partstore.CapabilityTxFreeGetPart,
		partstore.CapabilityTxFreePutPart,
		partstore.CapabilityTxFreeDeletePart,
	)
}

func (s *sftpPartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	_, span := s.tracer.Start(ctx, "sftpPartStore.GetPart")
	defer span.End()

	filename := s.getFilename(partId)
	f, lease, err := doRetriableWithClient(ctx, s, func(client *sftp.Client) (*sftp.File, error) {
		f, err := client.OpenFile(filename, os.O_RDONLY)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil, partstore.ErrPartNotFound
			}
			return nil, err
		}
		return f, nil
	}, func(err error) bool {
		return errors.Is(err, partstore.ErrPartNotFound)
	})
	if err != nil {
		return nil, err
	}

	return &leasedReadCloser{
		ReadCloser: ioutils.NewReadAheadReadSeekCloser(f, readAheadBlockSize),
		lease:      lease,
	}, nil
}

// leasedReadCloser keeps the client of an open part leased until the part
// is closed, so the least-busy pick accounts for running downloads.
type leasedReadCloser struct {
	io.ReadCloser
	lease     *clientLease
	closeOnce sync.Once
}

func (r *leasedReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.closeOnce.Do(r.lease.release)
	return err
}

func (s *sftpPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	_, span := s.tracer.Start(ctx, "sftpPartStore.GetPartIds")
	defer span.End()

	dirEntries, lease, err := doRetriableWithClient(ctx, s, func(client *sftp.Client) ([]os.FileInfo, error) {
		return client.ReadDir(s.root)
	}, nil)
	if err != nil {
		return nil, err
	}
	lease.release()
	partIds := []partstore.PartId{}
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() {
			continue
		}
		if partId, ok := s.tryGetPartIdFromFilename(dirEntry.Name()); ok {
			partIds = append(partIds, *partId)
		}
	}
	return partIds, nil
}

func (s *sftpPartStore) Stats(ctx context.Context, tx database.Tx) (partstore.Stats, error) {
	dirEntries, lease, err := doRetriableWithClient(ctx, s, func(client *sftp.Client) ([]os.FileInfo, error) {
		return client.ReadDir(s.root)
	}, nil)
	if err != nil {
		return partstore.Stats{}, err
	}
	lease.release()
	var stats partstore.Stats
	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}
		if _, ok := s.tryGetPartIdFromFilename(entry.Name()); !ok {
			continue
		}
		stats.Parts++
		stats.Bytes += entry.Size()
	}
	return stats, nil
}

func (s *sftpPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	_, span := s.tracer.Start(ctx, "sftpPartStore.DeletePart")
	defer span.End()

	filename := s.getFilename(partId)
	if tx != nil {
		// Do not destroy data that the database may still reference after a
		// rollback or crash. Physical cleanup follows the logical commit.
		tx.OnAfterCommit(func(opCtx context.Context) error {
			return s.DeletePart(opCtx, nil, partId)
		})
		return nil
	}

	_, lease, err := doRetriableWithClient(ctx, s, func(client *sftp.Client) (*struct{}, error) {
		return nil, client.Remove(filename)
	}, func(err error) bool {
		// Check for both ENOENT and fs.ErrNotExist to be safe
		return errors.Is(err, syscall.ENOENT) || errors.Is(err, fs.ErrNotExist)
	})
	if err != nil {
		// If the error is "file not found", we consider it a success (idempotent delete)
		if errors.Is(err, syscall.ENOENT) || errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	lease.release()
	return nil
}

func isNotFoundError(err error) bool {
	return errors.Is(err, syscall.ENOENT) || errors.Is(err, fs.ErrNotExist)
}

// isStatusError reports whether err is a status the server answered with,
// as opposed to a connection failure worth retrying. pkg/sftp maps some
// statuses to os errors.
func isStatusError(err error) bool {
	var statusErr *sftp.StatusError
	return errors.As(err, &statusErr) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission)
}
