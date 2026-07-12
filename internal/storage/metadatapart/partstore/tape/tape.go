// Package tape implements a partstore.PartStore backed by a sequential-access
// tape device (internal/tape) with a durable disk journal in front of it.
//
// Writes land first in the disk journal (crash-safe, random-access); a
// background migrator later packs committed parts into sealed tape segments.
// Correctness is reconstructable from the journal and tape alone, without the
// database: on Start the store scans committed tape segments and the journal,
// merges their logical operations by sequence, and rebuilds the live index.
//
// A part is readable only once its payload and commit record are durable; an
// interrupted write is never mistaken for a committed part; an interrupted
// overwrite never replaces the previous committed generation; a tape segment
// is trusted only after a valid footer and commit marker.
package tape

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
)

const defaultRecordSize = 256 << 10

// DeviceOpener opens the tape device backing the store; it is called during
// Start so that slow device operations (cartridge load) happen at lifecycle
// start, not at construction.
type DeviceOpener func(ctx context.Context) (tapedev.Device, error)

// partLocation is where a live part's payload currently resides.
type partLocation int

const (
	locationJournal partLocation = iota
	locationTape
)

type indexEntry struct {
	generation journal.GenerationID
	length     uint64
	hash       [32]byte
	location   partLocation
	journalLoc journal.Locator
	tapeBlock  uint64
}

type tapePartStore struct {
	*lifecycle.ValidatedLifecycle
	deviceOpener DeviceOpener
	journalDir   string
	cacheDir     string
	recordSize   int
	durability   journal.DurabilityMode
	groupCommit  journal.GroupCommitPolicy
	policy       SegmentPackingPolicy
	tracer       trace.Tracer

	mu       sync.Mutex // guards device, journal, migrator, index
	device   tapedev.Device
	journal  *journal.Journal
	migrator *Migrator
	index    map[partstore.PartId]indexEntry

	trigger      chan struct{}
	workerCancel context.CancelFunc
	workerDone   chan struct{}
}

var _ partstore.PartStore = (*tapePartStore)(nil)

type Option func(*tapePartStore) error

// WithRecordSize sets the tape record (block) size used when chunking part
// content into a segment.
func WithRecordSize(n int) Option {
	return func(s *tapePartStore) error {
		if n <= 0 || n > 1<<30 {
			return fmt.Errorf("invalid tape record size %d", n)
		}
		s.recordSize = n
		return nil
	}
}

// WithJournalDir sets the directory of the durable disk journal (required).
func WithJournalDir(dir string) Option {
	return func(s *tapePartStore) error {
		if dir == "" {
			return errors.New("journal directory must not be empty")
		}
		s.journalDir = dir
		return nil
	}
}

// WithReadCacheDir sets the mandatory disk staging cache used for
// tape-resident parts. When omitted it lives below the journal directory.
func WithReadCacheDir(dir string) Option {
	return func(s *tapePartStore) error {
		if dir == "" {
			return errors.New("read cache directory must not be empty")
		}
		s.cacheDir = dir
		return nil
	}
}

// WithDurability selects the journal durability mode.
func WithDurability(mode journal.DurabilityMode) Option {
	return func(s *tapePartStore) error {
		s.durability = mode
		return nil
	}
}

// WithGroupCommit sets the group-commit policy (used with DurabilityGroupCommit).
func WithGroupCommit(policy journal.GroupCommitPolicy) Option {
	return func(s *tapePartStore) error {
		s.groupCommit = policy
		return nil
	}
}

// WithPackingPolicy sets the segment packing policy.
func WithPackingPolicy(policy SegmentPackingPolicy) Option {
	return func(s *tapePartStore) error {
		s.policy = policy
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
		policy:             DefaultPackingPolicy(),
		tracer:             otel.Tracer("internal/storage/metadatapart/partstore/tape"),
		trigger:            make(chan struct{}, 1),
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	if s.journalDir == "" {
		return nil, errors.New("tape part store requires a journal directory (WithJournalDir)")
	}
	if s.cacheDir == "" {
		s.cacheDir = filepath.Join(s.journalDir, "read-cache")
	}
	return s, nil
}

func (s *tapePartStore) Start(ctx context.Context) error {
	ctx, span := s.tracer.Start(ctx, "tapePartStore.Start")
	defer span.End()

	if err := s.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}

	device, err := s.deviceOpener(ctx)
	if err != nil {
		return fmt.Errorf("opening tape device: %w", err)
	}
	j, err := journal.Open(journal.Options{
		Dir:         s.journalDir,
		Durability:  s.durability,
		GroupCommit: s.groupCommit,
	})
	if err != nil {
		_ = device.Close()
		return fmt.Errorf("opening journal: %w", err)
	}
	if err := os.MkdirAll(s.cacheDir, 0o755); err != nil {
		_ = j.Close()
		_ = device.Close()
		return fmt.Errorf("creating tape read cache: %w", err)
	}

	s.mu.Lock()
	s.device = device
	s.journal = j
	if err := s.recoverLocked(ctx); err != nil {
		s.mu.Unlock()
		_ = j.Close()
		_ = device.Close()
		return fmt.Errorf("recovering tape part store: %w", err)
	}
	s.mu.Unlock()

	workerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.workerCancel = cancel
	s.workerDone = make(chan struct{})
	go func() {
		defer close(s.workerDone)
		s.migrationLoop(workerCtx)
	}()
	return nil
}

// recoverLocked rebuilds the live index from committed tape segments and the
// journal, without the database. Must hold s.mu.
func (s *tapePartStore) recoverLocked(ctx context.Context) error {
	started := time.Now()
	slog.InfoContext(ctx, "Recovering tape part store")

	segments, tailBlock, err := scanSegments(ctx, s.device)
	if err != nil {
		return fmt.Errorf("scanning tape segments: %w", err)
	}
	if err := s.sealTornTailLocked(ctx, tailBlock); err != nil {
		slog.WarnContext(ctx, "Failed to seal untrusted tape tail", "error", err)
	}

	snap, err := s.journal.Snapshot()
	if err != nil {
		return fmt.Errorf("scanning journal: %w", err)
	}

	// Tape payload locations by generation, and the tape's logical operations.
	tapeLoc := map[journal.GenerationID]indexEntry{}
	var previousSegment [16]byte
	var nextSequence uint64 = 1
	type mergedOp struct {
		sequence           uint64
		isActivate         bool
		partID             partstore.PartId
		generation         journal.GenerationID
		expectedPrevious   *journal.GenerationID
		expectedGeneration *journal.GenerationID
	}
	opBySeq := map[uint64]mergedOp{}

	for _, seg := range segments {
		for _, e := range seg.index {
			tapeLoc[e.generation] = indexEntry{
				generation: e.generation,
				length:     e.dataLength,
				hash:       e.dataHash,
				location:   locationTape,
				tapeBlock:  e.startBlock,
			}
		}
		for _, a := range seg.activates {
			opBySeq[a.sequence] = mergedOp{sequence: a.sequence, isActivate: true, partID: a.partID, generation: a.generation, expectedPrevious: a.expectedPrevious}
		}
		for _, d := range seg.deletes {
			opBySeq[d.sequence] = mergedOp{sequence: d.sequence, isActivate: false, partID: d.partID, expectedGeneration: d.expectedGeneration}
		}
		previousSegment = seg.header.segmentID
		if end := seg.header.sequenceStart + seg.footer.recordCount + 2; end > nextSequence {
			nextSequence = end
		}
	}

	// Merge in the journal's logical operations (deduplicated by sequence with
	// the tape's, since a migrated activation appears in both).
	for _, op := range snap.Ops {
		if _, ok := opBySeq[op.Sequence]; ok {
			continue
		}
		opBySeq[op.Sequence] = mergedOp{
			sequence:           op.Sequence,
			isActivate:         op.IsActivate(),
			partID:             op.PartID,
			generation:         op.Generation,
			expectedPrevious:   op.ExpectedPrevious,
			expectedGeneration: op.ExpectedGeneration,
		}
	}

	// Replay the merged operation stream in sequence order to derive the live
	// generation of each part. Physical existence never implies visibility.
	ops := make([]mergedOp, 0, len(opBySeq))
	for _, op := range opBySeq {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(a, b int) bool { return ops[a].sequence < ops[b].sequence })

	live := map[partstore.PartId]journal.GenerationID{}
	for _, op := range ops {
		if op.isActivate {
			if _, onTape := tapeLoc[op.generation]; !onTape {
				if _, inJournal := snap.Parts[op.generation]; !inJournal {
					// The activation references a payload that is not durably
					// committed anywhere; keep the previous live generation.
					continue
				}
			}
			if op.expectedPrevious != nil {
				cur, ok := live[op.partID]
				if !ok || cur != *op.expectedPrevious {
					continue
				}
			}
			live[op.partID] = op.generation
		} else {
			if op.expectedGeneration != nil {
				cur, ok := live[op.partID]
				if !ok || cur != *op.expectedGeneration {
					continue
				}
			}
			delete(live, op.partID)
		}
	}

	// Build the in-memory index, preferring the tape copy (authoritative once
	// checkpointed) and falling back to the journal copy.
	s.index = make(map[partstore.PartId]indexEntry, len(live))
	corrupt := 0
	for partID, gen := range live {
		if entry, ok := tapeLoc[gen]; ok {
			s.index[partID] = entry
			continue
		}
		if part, ok := snap.Parts[gen]; ok {
			s.index[partID] = indexEntry{
				generation: gen,
				length:     part.Length,
				hash:       part.Hash,
				location:   locationJournal,
				journalLoc: part.Location,
			}
			continue
		}
		// A live generation whose payload cannot be found: report and skip
		// rather than expose a broken part.
		corrupt++
		slog.WarnContext(ctx, "Live part has no recoverable payload", "partId", partID.String(), "generation", gen.String())
	}

	s.migrator = NewMigrator(s.journal, s.device, s.recordSize, s.policy, previousSegment, nextSequence)

	slog.InfoContext(ctx, "Tape part store recovered",
		"elapsed", time.Since(started).Round(time.Millisecond),
		"segments", len(segments),
		"liveParts", len(s.index),
		"corruptParts", corrupt,
	)
	return nil
}

// sealTornTailLocked erases an untrusted tail after the last committed segment
// by writing a filemark at the clean append point. Must hold s.mu.
func (s *tapePartStore) sealTornTailLocked(ctx context.Context, tailBlock uint64) error {
	if err := s.device.SeekToEOD(ctx); err != nil {
		return err
	}
	pos, err := s.device.Tell(ctx)
	if err != nil {
		return err
	}
	if pos.Block <= tailBlock {
		return nil // nothing untrusted beyond the last committed segment
	}
	slog.WarnContext(ctx, "Sealing untrusted tape tail", "tailBlock", tailBlock, "eod", pos.Block)
	if err := s.device.LocateBlock(ctx, tailBlock); err != nil {
		return err
	}
	if err := s.device.WriteFilemarks(ctx, 1); err != nil {
		return err
	}
	return s.device.Flush(ctx)
}

func (s *tapePartStore) Stop(ctx context.Context) error {
	_, span := s.tracer.Start(ctx, "tapePartStore.Stop")
	defer span.End()

	if s.workerCancel != nil {
		s.workerCancel()
	}
	if s.workerDone != nil {
		<-s.workerDone
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	if s.journal != nil {
		if err := s.journal.Close(); err != nil {
			firstErr = err
		}
		s.journal = nil
	}
	if s.device != nil {
		if err := s.device.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.device = nil
	}
	if err := s.ValidatedLifecycle.Stop(ctx); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (s *tapePartStore) checkStarted() error {
	if s.device == nil || s.journal == nil {
		return errors.New("tapePartStore is not started")
	}
	return nil
}

func (s *tapePartStore) SupportsTxFreeGetPart() bool    { return true }
func (s *tapePartStore) SupportsTxFreeDeletePart() bool { return true }

func (s *tapePartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, options partstore.PutPartOptions, reader io.Reader) error {
	ctx, span := s.tracer.Start(ctx, "tapePartStore.PutPart")
	defer span.End()

	if err := options.Placement.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	if err := s.checkStarted(); err != nil {
		s.mu.Unlock()
		return err
	}
	j := s.journal
	var prevGen *journal.GenerationID
	if entry, ok := s.index[partId]; ok {
		g := entry.generation
		prevGen = &g
	}
	s.mu.Unlock()

	gen, err := journal.NewGenerationID()
	if err != nil {
		return err
	}
	input := journal.PartInput{
		Generation: gen,
		PartID:     partId,
		ObjectID:   options.Placement.ObjectID,
		PartNumber: options.Placement.PartNumber,
		PartCount:  options.Placement.PartCount,
		ObjectSize: options.Placement.ObjectSize,
	}
	loc, err := j.WritePart(ctx, input, reader)
	if err != nil {
		return fmt.Errorf("staging part %s to journal: %w", partId.String(), err)
	}
	newEntry := indexEntry{generation: gen, length: loc.Length, hash: loc.Hash, location: locationJournal, journalLoc: loc}

	if tx == nil {
		if _, err := j.Activate(ctx, partId, gen, prevGen); err != nil {
			return err
		}
		s.mu.Lock()
		s.index[partId] = newEntry
		s.mu.Unlock()
		s.triggerMigration()
		return nil
	}

	activated := false
	tx.OnPreCommit(func(ctx context.Context) error {
		// Activation is the logical commit point: durable before the database
		// commits, so a crash after this leaves a live orphan (not data loss)
		// that GC condemns later.
		if _, err := j.Activate(ctx, partId, gen, prevGen); err != nil {
			return err
		}
		activated = true
		return nil
	})
	tx.OnAfterCommit(func(context.Context) error {
		s.mu.Lock()
		s.index[partId] = newEntry
		s.mu.Unlock()
		s.triggerMigration()
		return nil
	})
	tx.OnRollback(func(ctx context.Context) error {
		if !activated {
			return nil
		}
		// The activation was made durable in pre-commit but the transaction
		// rolled back: append a compensating logical op restoring the previous
		// generation (or deleting the part if there was none).
		if prevGen != nil {
			_, err := j.Activate(ctx, partId, *prevGen, &gen)
			return err
		}
		_, err := j.Delete(ctx, partId, &gen)
		return err
	})
	return nil
}

func (s *tapePartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := s.tracer.Start(ctx, "tapePartStore.GetPart")
	defer span.End()

	s.mu.Lock()
	if err := s.checkStarted(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	entry, ok := s.index[partId]
	j := s.journal
	s.mu.Unlock()
	if !ok {
		return nil, partstore.ErrPartNotFound
	}
	if entry.location == locationJournal {
		return j.OpenPayload(entry.journalLoc)
	}
	return s.openCachedTapePart(ctx, partId, entry)
}

func (s *tapePartStore) newTapePartReader(ctx context.Context, partId partstore.PartId, entry indexEntry) *tapePartReader {
	return &tapePartReader{
		store:              s,
		ctx:                ctx,
		partId:             partId,
		expectedGeneration: entry.generation,
		expectedLength:     entry.length,
		expectedHash:       entry.hash,
		nextBlock:          entry.tapeBlock,
		buf:                make([]byte, max(segControlBufferSize, s.recordSize+segEnvelopeSize+segPayloadCRC)),
		hasher:             sha256.New(),
	}
}

// openCachedTapePart makes disk staging mandatory for tape-resident payloads.
// A complete immutable generation is written and synced before it becomes
// visible in the cache, so range readers never need to reposition tape.
func (s *tapePartStore) openCachedTapePart(ctx context.Context, partID partstore.PartId, entry indexEntry) (io.ReadCloser, error) {
	path := filepath.Join(s.cacheDir, entry.generation.String()+".part")
	if info, err := os.Stat(path); err == nil && uint64(info.Size()) == entry.length {
		return os.Open(path)
	}

	tmp, err := os.CreateTemp(s.cacheDir, ".staging-*")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	reader := s.newTapePartReader(ctx, partID, entry)
	written, copyErr := io.Copy(tmp, reader)
	closeErr := reader.Close()
	if copyErr != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("staging tape part %s: %w", partID.String(), copyErr)
	}
	if closeErr != nil {
		_ = tmp.Close()
		return nil, closeErr
	}
	if uint64(written) != entry.length {
		_ = tmp.Close()
		return nil, fmt.Errorf("staging tape part %s: short copy %d of %d bytes", partID.String(), written, entry.length)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		// Another reader may have completed the same immutable generation.
		if _, statErr := os.Stat(path); statErr != nil {
			return nil, err
		}
	}
	return os.Open(path)
}

func (s *tapePartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	_, span := s.tracer.Start(ctx, "tapePartStore.GetPartIds")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkStarted(); err != nil {
		return nil, err
	}
	ids := make([]partstore.PartId, 0, len(s.index))
	for id := range s.index {
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *tapePartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	ctx, span := s.tracer.Start(ctx, "tapePartStore.DeletePart")
	defer span.End()

	apply := func(ctx context.Context) error {
		s.mu.Lock()
		if err := s.checkStarted(); err != nil {
			s.mu.Unlock()
			return err
		}
		entry, ok := s.index[partId]
		j := s.journal
		s.mu.Unlock()
		if !ok {
			return nil
		}
		gen := entry.generation
		if _, err := j.Delete(ctx, partId, &gen); err != nil {
			return err
		}
		s.mu.Lock()
		if cur, ok := s.index[partId]; ok && cur.generation == gen {
			delete(s.index, partId)
		}
		s.mu.Unlock()
		return nil
	}
	if tx == nil {
		return apply(ctx)
	}
	// Deletion is applied only after the database commits: a delete applied
	// before a failed commit would lose data, whereas a delete lost after
	// commit leaves an orphan that GC reclaims.
	tx.OnAfterCommit(apply)
	return nil
}

// triggerMigration nudges the background worker without blocking.
func (s *tapePartStore) triggerMigration() {
	select {
	case s.trigger <- struct{}{}:
	default:
	}
}

func (s *tapePartStore) migrationLoop(ctx context.Context) {
	interval := s.policy.MaxWait
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.trigger:
			s.runMigration(ctx, false)
		case <-ticker.C:
			// Periodic flush forces out parts that have waited past MaxWait.
			s.runMigration(ctx, true)
		}
	}
}

// runMigration migrates one segment and updates the index for migrated parts.
func (s *tapePartStore) runMigration(ctx context.Context, force bool) {
	s.mu.Lock()
	migrator := s.migrator
	s.mu.Unlock()
	if migrator == nil {
		return
	}
	res, err := migrator.MigrateOnce(ctx, force)
	if err != nil {
		slog.WarnContext(ctx, "Tape migration failed", "error", err)
		return
	}
	if len(res.Parts) == 0 {
		return
	}
	s.mu.Lock()
	for _, p := range res.Parts {
		if entry, ok := s.index[p.PartID]; ok && entry.generation == p.Generation {
			entry.location = locationTape
			entry.tapeBlock = p.StartBlock
			s.index[p.PartID] = entry
		}
	}
	s.mu.Unlock()
}

// tapePartReader streams a migrated part's data records from tape. It holds no
// device resources between Reads: each Read acquires the store mutex,
// repositions the head and reads one record, so multiple open readers can be
// drained in any order on one goroutine without deadlocking.
type tapePartReader struct {
	store              *tapePartStore
	ctx                context.Context
	partId             partstore.PartId
	expectedGeneration journal.GenerationID
	expectedLength     uint64
	expectedHash       [32]byte
	bytesRead          uint64
	hasher             hash.Hash
	nextBlock          uint64
	headerVerified     bool
	buf                []byte
	pending            []byte
	eof                bool
	closed             bool
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

func (r *tapePartReader) fill() error {
	s := r.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.device == nil {
		return tapedev.ErrClosed
	}
	if err := r.positionLocked(); err != nil {
		return err
	}
	if !r.headerVerified {
		n, err := s.device.ReadRecord(r.ctx, r.buf)
		if err != nil {
			return fmt.Errorf("reading tape part-begin at block %d: %w", r.nextBlock, err)
		}
		rec, err := decodeSegmentRecord(r.buf[:n])
		if err != nil || rec.kind != segKindPartBegin {
			return fmt.Errorf("tape part %s: index/tape mismatch at block %d", r.partId.String(), r.nextBlock)
		}
		begin, err := decodeSegPartBegin(rec.payload)
		if err != nil || !begin.partID.Equal(r.partId) || begin.generation != r.expectedGeneration {
			return fmt.Errorf("tape part %s: unexpected part at block %d", r.partId.String(), r.nextBlock)
		}
		r.headerVerified = true
		r.nextBlock++
	}
	n, err := s.device.ReadRecord(r.ctx, r.buf)
	if err != nil {
		return fmt.Errorf("reading tape part data at block %d: %w", r.nextBlock, err)
	}
	rec, err := decodeSegmentRecord(r.buf[:n])
	if err != nil {
		return fmt.Errorf("decoding tape record at block %d: %w", r.nextBlock, err)
	}
	if rec.kind == segKindPartEnd {
		end, err := decodeSegPartEnd(rec.payload)
		if err != nil || end.generation != r.expectedGeneration || end.dataLength != r.expectedLength || end.dataHash != r.expectedHash || r.bytesRead != r.expectedLength {
			return fmt.Errorf("tape part %s: invalid part-end at block %d", r.partId.String(), r.nextBlock)
		}
		var actualHash [32]byte
		copy(actualHash[:], r.hasher.Sum(nil))
		if actualHash != r.expectedHash {
			return fmt.Errorf("tape part %s: content hash mismatch", r.partId.String())
		}
		r.eof = true
		return nil
	}
	if rec.kind != segKindPartData {
		return fmt.Errorf("tape part %s: unexpected record kind %d at block %d", r.partId.String(), rec.kind, r.nextBlock)
	}
	r.nextBlock++
	r.bytesRead += uint64(len(rec.payload))
	_, _ = r.hasher.Write(rec.payload)
	// Copy out of the shared buffer: pending must survive the next fill.
	r.pending = append(r.pending[:0], rec.payload...)
	return nil
}

// positionLocked keeps the tape streaming when this reader (or the next
// physically adjacent part reader) already owns the head. A part ends just
// before its PART_COMMIT record; consuming that one control record lets the
// next part begin without a costly LocateBlock. Must hold store.mu.
func (r *tapePartReader) positionLocked() error {
	pos, err := r.store.device.Tell(r.ctx)
	if err != nil {
		return err
	}
	if pos.Block == r.nextBlock {
		return nil
	}
	if !r.headerVerified && pos.Block+1 == r.nextBlock {
		n, err := r.store.device.ReadRecord(r.ctx, r.buf)
		if err != nil {
			return err
		}
		rec, err := decodeSegmentRecord(r.buf[:n])
		if err != nil || rec.kind != segKindPartCommit {
			return fmt.Errorf("tape part %s: expected adjacent part commit at block %d", r.partId.String(), pos.Block)
		}
		return nil
	}
	return r.store.device.LocateBlock(r.ctx, r.nextBlock)
}

func (r *tapePartReader) Close() error {
	r.closed = true
	return nil
}
