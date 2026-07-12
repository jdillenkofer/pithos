// Package tape implements a partstore.PartStore on a sequential-access tape
// device (internal/tape). Parts are appended at end-of-data as self-describing
// segments (a header record carrying the partId, data records, a filemark);
// deletions only append small tombstone segments. The partId->position index
// is held in memory only and rebuilt on Start by scanning the tape.
//
// Accepted trade-offs of the append-only design: Start scans the whole tape;
// dead bytes accumulate from overwrites, rollbacks and tombstones (compaction
// is future work); a crash between a tape append and its transaction hook can
// leave an orphaned part on tape, which the garbage collector condemns and
// deletes later.
package tape

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
)

const defaultRecordSize = 256 << 10

const indexRebuildProgressInterval = 10 * time.Second

// DeviceOpener opens the tape device backing the store; it is called during
// Start so that slow device operations (cartridge load) happen at lifecycle
// start, not at construction.
type DeviceOpener func(ctx context.Context) (tapedev.Device, error)

type tapePartStore struct {
	*lifecycle.ValidatedLifecycle
	deviceOpener DeviceOpener
	recordSize   int
	tracer       trace.Tracer

	mu     sync.Mutex // guards the device head and the index
	device tapedev.Device
	index  map[partstore.PartId]uint64 // partId -> start block of the newest committed copy
}

// Compile-time check to ensure tapePartStore implements partstore.PartStore
var _ partstore.PartStore = (*tapePartStore)(nil)

type Option func(*tapePartStore) error

// WithRecordSize sets the data record (tape block) size used when chunking
// part content.
func WithRecordSize(n int) Option {
	return func(s *tapePartStore) error {
		if n <= 0 || n > 1<<30 {
			return fmt.Errorf("invalid tape record size %d", n)
		}
		s.recordSize = n
		return nil
	}
}

func New(deviceOpener DeviceOpener, opts ...Option) (partstore.PartStore, error) {
	if deviceOpener == nil {
		return nil, errors.New("deviceOpener must not be nil")
	}
	validatedLifecycle, err := lifecycle.NewValidatedLifecycle("tapePartStore")
	if err != nil {
		return nil, err
	}
	s := &tapePartStore{
		ValidatedLifecycle: validatedLifecycle,
		deviceOpener:       deviceOpener,
		recordSize:         defaultRecordSize,
		tracer:             otel.Tracer("internal/storage/metadatapart/partstore/tape"),
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *tapePartStore) Start(ctx context.Context) error {
	ctx, span := s.tracer.Start(ctx, "tapePartStore.Start")
	defer span.End()

	if err := s.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	loadStarted := time.Now()
	slog.InfoContext(ctx, "Loading tape device")
	device, err := s.deviceOpener(ctx)
	if err != nil {
		return fmt.Errorf("opening tape device: %w", err)
	}
	slog.InfoContext(ctx, "Tape device loaded", "elapsed", time.Since(loadStarted).Round(time.Millisecond))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.device = device
	if err := s.rebuildIndex(ctx); err != nil {
		s.device = nil
		_ = device.Close()
		return fmt.Errorf("rebuilding tape part index: %w", err)
	}
	return nil
}

// rebuildIndex scans the tape from the beginning, replaying data segments and
// tombstones into the in-memory index. A segment truncated by a crash (no
// terminating filemark) is not indexed and gets sealed with a filemark and a
// tombstone so later appends stay parseable. Must be called with mu held.
func (s *tapePartStore) rebuildIndex(ctx context.Context) error {
	started := time.Now()
	slog.InfoContext(ctx, "Rebuilding tape part index")
	if err := s.device.Rewind(ctx); err != nil {
		return err
	}

	liveCopies := map[partstore.PartId]map[uint64]struct{}{}
	var scannedSegments uint64
	var dataSegments uint64
	var tombstones uint64
	lastProgress := started
	logProgress := func(block uint64) {
		if time.Since(lastProgress) < indexRebuildProgressInterval {
			return
		}
		slog.InfoContext(ctx, "Rebuilding tape part index",
			"elapsed", time.Since(started).Round(time.Second),
			"block", block,
			"segments", scannedSegments,
			"dataSegments", dataSegments,
			"tombstones", tombstones,
			"liveParts", len(liveCopies),
		)
		lastProgress = time.Now()
	}
	needFilemarkSeal := false
	var sealTombstone *header
	buf := make([]byte, headerBufferSize)

scan:
	for {
		pos, err := s.device.Tell(ctx)
		if err != nil {
			return err
		}
		n, err := s.device.ReadRecord(ctx, buf)
		switch {
		case errors.Is(err, tapedev.ErrEndOfData):
			break scan
		case errors.Is(err, tapedev.ErrFilemark):
			// Stray filemark (empty segment), skip it.
			logProgress(pos.Block)
			continue
		case errors.Is(err, io.ErrShortBuffer):
			scannedSegments++
			// Too large for a header: not written by this store.
			slog.Warn("Skipping unrecognized tape segment", "block", pos.Block)
			if skipErr := s.device.SpaceFilemarks(ctx, 1); skipErr != nil {
				if errors.Is(skipErr, tapedev.ErrEndOfData) {
					needFilemarkSeal = true
					break scan
				}
				return skipErr
			}
			logProgress(pos.Block)
			continue
		case err != nil:
			return err
		}

		h, err := decodeHeader(buf[:n])
		if err != nil {
			scannedSegments++
			slog.Warn("Skipping unparseable tape segment header", "block", pos.Block, "error", err)
			if skipErr := s.device.SpaceFilemarks(ctx, 1); skipErr != nil {
				if errors.Is(skipErr, tapedev.ErrEndOfData) {
					needFilemarkSeal = true
					break scan
				}
				return skipErr
			}
			logProgress(pos.Block)
			continue
		}
		scannedSegments++

		truncated := false
		if err := s.device.SpaceFilemarks(ctx, 1); err != nil {
			if !errors.Is(err, tapedev.ErrEndOfData) {
				return err
			}
			truncated = true
		}

		switch h.kind {
		case kindData:
			dataSegments++
			if truncated {
				// Crash mid-PutPart: the part is incomplete, seal it away.
				slog.Warn("Sealing truncated tape part segment", "partId", h.partId.String(), "block", pos.Block)
				needFilemarkSeal = true
				sealTombstone = &header{partId: h.partId, invalidatedBlock: pos.Block}
				break scan
			}
			if liveCopies[h.partId] == nil {
				liveCopies[h.partId] = map[uint64]struct{}{}
			}
			liveCopies[h.partId][pos.Block] = struct{}{}
		case kindTombstone:
			tombstones++
			// A tombstone record is atomic; apply it even if its filemark
			// is missing.
			if h.invalidatedBlock == tombstoneAllCopies {
				delete(liveCopies, h.partId)
			} else if copies, ok := liveCopies[h.partId]; ok {
				delete(copies, h.invalidatedBlock)
				if len(copies) == 0 {
					delete(liveCopies, h.partId)
				}
			}
			if truncated {
				needFilemarkSeal = true
				break scan
			}
		}
		logProgress(pos.Block)
	}

	s.index = make(map[partstore.PartId]uint64, len(liveCopies))
	for partId, copies := range liveCopies {
		newest := uint64(0)
		for block := range copies {
			if block >= newest {
				newest = block
			}
		}
		s.index[partId] = newest
	}

	if needFilemarkSeal {
		// Best effort: if the tape cannot be written (full, protected), the
		// truncated tail stays last and the rebuild result stays correct.
		if err := s.sealTail(ctx, sealTombstone); err != nil {
			slog.Warn("Failed to seal truncated tape tail", "error", err)
		}
	}
	slog.InfoContext(ctx, "Tape part index rebuilt",
		"elapsed", time.Since(started).Round(time.Millisecond),
		"segments", scannedSegments,
		"dataSegments", dataSegments,
		"tombstones", tombstones,
		"liveParts", len(s.index),
	)
	return nil
}

func (s *tapePartStore) sealTail(ctx context.Context, sealTombstone *header) error {
	if err := s.device.SeekToEOD(ctx); err != nil {
		return err
	}
	if err := s.device.WriteFilemarks(ctx, 1); err != nil {
		return err
	}
	if sealTombstone != nil {
		if err := s.device.WriteRecord(ctx, encodeTombstone(sealTombstone.partId, sealTombstone.invalidatedBlock)); err != nil {
			return err
		}
		if err := s.device.WriteFilemarks(ctx, 1); err != nil {
			return err
		}
	}
	return nil
}

func (s *tapePartStore) Stop(ctx context.Context) error {
	_, span := s.tracer.Start(ctx, "tapePartStore.Stop")
	defer span.End()

	s.mu.Lock()
	if s.device != nil {
		if err := s.device.Close(); err != nil {
			s.mu.Unlock()
			return err
		}
		s.device = nil
	}
	s.mu.Unlock()
	return s.ValidatedLifecycle.Stop(ctx)
}

// appendPart writes a complete data segment at end-of-data and returns the
// block of its header record. Must be called with mu held.
func (s *tapePartStore) appendPart(ctx context.Context, partId partstore.PartId, reader io.Reader) (uint64, error) {
	if err := s.device.SeekToEOD(ctx); err != nil {
		return 0, err
	}
	pos, err := s.device.Tell(ctx)
	if err != nil {
		return 0, err
	}
	startBlock := pos.Block
	appendErr := func() error {
		if err := s.device.WriteRecord(ctx, encodeDataHeader(partId)); err != nil {
			return err
		}
		buf := make([]byte, s.recordSize)
		for {
			n, err := io.ReadFull(reader, buf)
			if n > 0 {
				if err := s.device.WriteRecord(ctx, buf[:n]); err != nil {
					return err
				}
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			if err != nil {
				return fmt.Errorf("reading part content: %w", err)
			}
		}
		return s.device.WriteFilemarks(ctx, 1)
	}()
	if appendErr != nil {
		// Best-effort seal so the incomplete segment cannot shadow the
		// index rebuild or later appends.
		if err := s.sealTail(ctx, &header{partId: partId, invalidatedBlock: startBlock}); err != nil {
			slog.Warn("Failed to seal aborted tape part segment", "partId", partId.String(), "error", err)
		}
		return 0, appendErr
	}
	return startBlock, nil
}

// appendTombstone writes a tombstone segment at end-of-data. Must be called
// with mu held.
func (s *tapePartStore) appendTombstone(ctx context.Context, partId partstore.PartId, invalidatedBlock uint64) error {
	if err := s.device.SeekToEOD(ctx); err != nil {
		return err
	}
	if err := s.device.WriteRecord(ctx, encodeTombstone(partId, invalidatedBlock)); err != nil {
		return err
	}
	return s.device.WriteFilemarks(ctx, 1)
}

func (s *tapePartStore) checkStarted() error {
	if s.device == nil {
		return errors.New("tapePartStore is not started")
	}
	return nil
}

func (s *tapePartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	ctx, span := s.tracer.Start(ctx, "tapePartStore.PutPart")
	defer span.End()

	s.mu.Lock()
	if err := s.checkStarted(); err != nil {
		s.mu.Unlock()
		return err
	}
	startBlock, err := s.appendPart(ctx, partId, reader)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("appending part %s to tape: %w", partId.String(), err)
	}
	if tx == nil {
		s.index[partId] = startBlock
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	tx.OnAfterCommit(func(context.Context) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.index[partId] = startBlock
		return nil
	})
	tx.OnRollback(func(ctx context.Context) error {
		// Best effort: a crash before this hook leaves an orphaned copy on
		// tape, which the index rebuild resurrects and GC condemns later.
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.device == nil {
			return nil
		}
		if err := s.appendTombstone(ctx, partId, startBlock); err != nil {
			slog.Warn("Failed to tombstone rolled-back tape part", "partId", partId.String(), "error", err)
		}
		return nil
	})
	return nil
}

// SupportsTxFreeGetPart reports that GetPart never uses the transaction.
func (s *tapePartStore) SupportsTxFreeGetPart() bool {
	return true
}

func (s *tapePartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := s.tracer.Start(ctx, "tapePartStore.GetPart")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkStarted(); err != nil {
		return nil, err
	}
	startBlock, ok := s.index[partId]
	if !ok {
		return nil, partstore.ErrPartNotFound
	}
	return &tapePartReader{
		store:     s,
		ctx:       ctx,
		partId:    partId,
		nextBlock: startBlock,
		buf:       make([]byte, s.recordSize),
	}, nil
}

func (s *tapePartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	_, span := s.tracer.Start(ctx, "tapePartStore.GetPartIds")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkStarted(); err != nil {
		return nil, err
	}
	partIds := make([]partstore.PartId, 0, len(s.index))
	for partId := range s.index {
		partIds = append(partIds, partId)
	}
	return partIds, nil
}

func (s *tapePartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	ctx, span := s.tracer.Start(ctx, "tapePartStore.DeletePart")
	defer span.End()

	apply := func(ctx context.Context) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := s.checkStarted(); err != nil {
			return err
		}
		if _, ok := s.index[partId]; !ok {
			// The part didn't exist
			return nil
		}
		if err := s.appendTombstone(ctx, partId, tombstoneAllCopies); err != nil {
			return fmt.Errorf("appending tombstone for part %s: %w", partId.String(), err)
		}
		delete(s.index, partId)
		return nil
	}
	if tx == nil {
		return apply(ctx)
	}
	// The tombstone is irreversible on an append-only medium, so it is only
	// written once the transaction has committed. A failure here leaves an
	// orphan on tape that GC condemns and deletes on a later run.
	tx.OnAfterCommit(apply)
	return nil
}

func (s *tapePartStore) SupportsTxFreeDeletePart() bool { return true }

// tapePartReader streams a part's data records. It holds no resources
// between Reads: each Read acquires the store mutex, repositions the head
// and reads one record, so multiple open readers can be drained in any
// order on one goroutine without deadlocking.
type tapePartReader struct {
	store          *tapePartStore
	ctx            context.Context
	partId         partstore.PartId
	nextBlock      uint64
	headerVerified bool
	buf            []byte
	pending        []byte
	eof            bool
	closed         bool
}

func (r *tapePartReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, fs.ErrClosed
	}
	for {
		if len(r.pending) > 0 {
			n := copy(p, r.pending)
			r.pending = r.pending[n:]
			return n, nil
		}
		if r.eof {
			return 0, io.EOF
		}
		if err := r.fill(); err != nil {
			return 0, err
		}
	}
}

// fill reads the next record of the part into the internal buffer.
func (r *tapePartReader) fill() error {
	s := r.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.device == nil {
		return tapedev.ErrClosed
	}
	if err := s.device.LocateBlock(r.ctx, r.nextBlock); err != nil {
		return err
	}
	if !r.headerVerified {
		buf := make([]byte, headerBufferSize)
		n, err := s.device.ReadRecord(r.ctx, buf)
		if err != nil {
			return fmt.Errorf("reading tape part header at block %d: %w", r.nextBlock, err)
		}
		h, err := decodeHeader(buf[:n])
		if err != nil || h.kind != kindData || !h.partId.Equal(r.partId) {
			return fmt.Errorf("tape part %s: index/tape mismatch at block %d", r.partId.String(), r.nextBlock)
		}
		r.headerVerified = true
		r.nextBlock++
	}
	n, err := s.device.ReadRecord(r.ctx, r.buf)
	switch {
	case errors.Is(err, tapedev.ErrFilemark):
		r.eof = true
		return nil
	case errors.Is(err, tapedev.ErrEndOfData):
		return io.ErrUnexpectedEOF
	case err != nil:
		return err
	}
	r.nextBlock++
	r.pending = r.buf[:n]
	return nil
}

func (r *tapePartReader) Close() error {
	r.closed = true
	return nil
}
