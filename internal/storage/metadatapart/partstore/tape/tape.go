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
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
)

const defaultRecordSize = 256 << 10
const defaultVolumeID = "pithos-volume-0"
const defaultReadCacheMaxBytes int64 = 100 << 30

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
	deviceOpener  DeviceOpener
	journalDir    string
	cacheDir      string
	cacheMaxBytes int64
	volumeID      string
	recordSize    int
	durability    journal.DurabilityMode
	groupCommit   journal.GroupCommitPolicy
	policy        SegmentPackingPolicy
	tracer        trace.Tracer
	partLocks     *partLocker

	lifecycleMu      sync.Mutex // serializes Start and Stop
	lifecycleStarted bool

	mu                 sync.Mutex // guards device, journal, migrator, index
	device             tapedev.Device
	journal            *journal.Journal
	migrator           *Migrator
	catalog            *tapeCatalog
	scheduler          *driveScheduler
	index              map[partstore.PartId]indexEntry
	cacheEntries       map[journal.GenerationID]*readCacheEntry
	cacheBytes         int64
	recalls            map[journal.GenerationID]*recallState
	lastMigrationError string
	endOfMedia         bool

	trigger      chan struct{}
	workerCancel context.CancelFunc
	workerDone   chan struct{}
}

var _ partstore.PartStore = (*tapePartStore)(nil)

// OperationalStatus is a point-in-time view suitable for health endpoints and
// operator alerts. EndOfMedia means tape migration is blocked but journal
// copies remain authoritative and readable.
type OperationalStatus struct {
	VolumeID            string
	MediaID             string
	Segments            int
	LiveParts           int
	JournalBacklogParts int
	JournalBacklogBytes uint64
	ReadCacheEntries    int
	ReadCacheBytes      int64
	DriveQueueDepth     int
	CurrentTapeBlock    uint64
	EndOfMedia          bool
	LastMigrationError  string
}

type Option func(*tapePartStore) error

// WithRecordSize sets the tape record (block) size used when chunking part
// content into a segment.
func WithRecordSize(n int) Option {
	return func(s *tapePartStore) error {
		if n <= 0 || n > segMaxPayloadSize {
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

// WithReadCacheMaxBytes bounds recalled payloads retained on disk. A single
// part larger than the bound is allowed temporarily so every valid part
// remains readable; it becomes the first eviction candidate afterward.
func WithReadCacheMaxBytes(bytes int64) Option {
	return func(s *tapePartStore) error {
		if bytes <= 0 {
			return errors.New("read cache max bytes must be positive")
		}
		s.cacheMaxBytes = bytes
		return nil
	}
}

// WithVolumeID sets the operator-facing cartridge identifier. The identifier
// is written into the permanent BOT label and prevents accidentally mounting a
// different cartridge under an existing catalog.
func WithVolumeID(id string) Option {
	return func(s *tapePartStore) error {
		if id == "" || id != strings.TrimSpace(id) || !utf8.ValidString(id) || len(id) > maxVolumeIDBytes {
			return fmt.Errorf("invalid tape volume id %q", id)
		}
		s.volumeID = id
		return nil
	}
}

// WithDurability selects the journal durability mode.
func WithDurability(mode journal.DurabilityMode) Option {
	return func(s *tapePartStore) error {
		if mode != journal.DurabilityPerPart && mode != journal.DurabilityGroupCommit {
			return fmt.Errorf("invalid tape journal durability mode %d", mode)
		}
		s.durability = mode
		return nil
	}
}

// WithGroupCommit sets the group-commit policy (used with DurabilityGroupCommit).
func WithGroupCommit(policy journal.GroupCommitPolicy) Option {
	return func(s *tapePartStore) error {
		if policy.MaxDelay < 0 || policy.MaxBytes < 0 {
			return errors.New("group commit delay and bytes must be non-negative")
		}
		s.groupCommit = policy
		return nil
	}
}

// WithPackingPolicy sets the segment packing policy.
func WithPackingPolicy(policy SegmentPackingPolicy) Option {
	return func(s *tapePartStore) error {
		if policy.TargetBytes <= 0 || policy.MaxBytes <= 0 || policy.TargetBytes > policy.MaxBytes {
			return errors.New("packing target/max bytes must be positive and target must not exceed max")
		}
		if policy.MaxWait < 0 || policy.MaxOpenObjects < 0 {
			return errors.New("packing max wait/open objects must be non-negative")
		}
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
		volumeID:           defaultVolumeID,
		cacheMaxBytes:      defaultReadCacheMaxBytes,
		recordSize:         defaultRecordSize,
		policy:             DefaultPackingPolicy(),
		tracer:             otel.Tracer("internal/storage/metadatapart/partstore/tape"),
		partLocks:          newPartLocker(),
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

	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.lifecycleStarted {
		return s.ValidatedLifecycle.Start(ctx)
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
	if err := s.initializeReadCache(); err != nil {
		_ = j.Close()
		_ = device.Close()
		return fmt.Errorf("initializing tape read cache: %w", err)
	}

	s.mu.Lock()
	s.device = device
	s.journal = j
	failInitialization := func(err error) error {
		s.device = nil
		s.journal = nil
		s.migrator = nil
		s.catalog = nil
		s.scheduler = nil
		s.index = nil
		s.mu.Unlock()
		_ = j.Close()
		_ = device.Close()
		return err
	}
	label, dataStart, err := openVolume(ctx, device, s.volumeID)
	if err != nil {
		return failInitialization(fmt.Errorf("opening tape volume: %w", err))
	}
	if err := s.recoverLocked(ctx, label, dataStart); err != nil {
		return failInitialization(fmt.Errorf("recovering tape part store: %w", err))
	}
	scheduler, err := newDriveScheduler(device)
	if err != nil {
		return failInitialization(fmt.Errorf("starting tape drive scheduler: %w", err))
	}
	if err := s.ValidatedLifecycle.Start(ctx); err != nil {
		s.mu.Unlock()
		_ = scheduler.stop(context.Background())
		s.mu.Lock()
		return failInitialization(err)
	}
	s.lifecycleStarted = true
	s.scheduler = scheduler
	workerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.workerCancel = cancel
	s.workerDone = make(chan struct{})
	workerDone := s.workerDone
	s.mu.Unlock()
	go func() {
		defer close(workerDone)
		s.migrationLoop(workerCtx)
	}()
	return nil
}

// recoverLocked rebuilds the live index from committed tape segments and the
// journal, without the database. Must hold s.mu.
func (s *tapePartStore) recoverLocked(ctx context.Context, label volumeLabel, dataStart uint64) error {
	started := time.Now()
	slog.InfoContext(ctx, "Recovering tape part store")

	catalog, err := loadTapeCatalog(s.journalDir, label)
	if err != nil {
		if errors.Is(err, ErrWrongTapeMedia) {
			return err
		}
		slog.WarnContext(ctx, "Tape catalog unavailable; rebuilding from tape manifests", "error", err)
		catalog = nil
	}
	if catalog == nil {
		catalog = newTapeCatalog(label, dataStart)
	}
	segments, err := catalog.scannedSegments()
	if err != nil {
		return fmt.Errorf("decoding tape catalog: %w", err)
	}
	newSegments, tailBlock, err := scanSegmentsFrom(ctx, s.device, catalog.TailBlock, catalog.PreviousSegment)
	if err != nil {
		return fmt.Errorf("scanning tape segments: %w", err)
	}
	for _, segment := range newSegments {
		catalog.appendSegment(segment)
	}
	segments = append(segments, newSegments...)
	if err := saveTapeCatalog(s.journalDir, catalog); err != nil {
		return fmt.Errorf("saving tape catalog: %w", err)
	}
	s.catalog = catalog
	if err := s.sealTornTailLocked(ctx, tailBlock); err != nil {
		return fmt.Errorf("sealing untrusted tape tail: %w", err)
	}

	// Tape payload locations by generation, and the tape's logical operations.
	tapeLoc := map[journal.GenerationID]indexEntry{}
	var previousSegment [16]byte
	var nextSequence uint64 = 1
	capturedOps := make(map[partstore.PartId]capturedLogicalState)
	type mergedOp struct {
		sequence           uint64
		isActivate         bool
		partID             partstore.PartId
		generation         journal.GenerationID
		expectedPrevious   *journal.GenerationID
		expectedGeneration *journal.GenerationID
	}
	opBySeq := map[uint64]mergedOp{}
	var maxTapeLogicalSequence uint64

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
			if _, duplicate := opBySeq[a.sequence]; duplicate {
				return fmt.Errorf("%w: duplicate logical sequence %d", ErrCorruptTape, a.sequence)
			}
			opBySeq[a.sequence] = mergedOp{sequence: a.sequence, isActivate: true, partID: a.partID, generation: a.generation, expectedPrevious: a.expectedPrevious}
			if a.sequence > capturedOps[a.partID].sequence {
				capturedOps[a.partID] = capturedLogicalState{sequence: a.sequence, activate: true, generation: a.generation}
			}
			maxTapeLogicalSequence = max(maxTapeLogicalSequence, a.sequence)
		}
		for _, d := range seg.deletes {
			if _, duplicate := opBySeq[d.sequence]; duplicate {
				return fmt.Errorf("%w: duplicate logical sequence %d", ErrCorruptTape, d.sequence)
			}
			opBySeq[d.sequence] = mergedOp{sequence: d.sequence, isActivate: false, partID: d.partID, expectedGeneration: d.expectedGeneration}
			if d.sequence > capturedOps[d.partID].sequence {
				capturedOps[d.partID] = capturedLogicalState{sequence: d.sequence}
			}
			maxTapeLogicalSequence = max(maxTapeLogicalSequence, d.sequence)
		}
		previousSegment = seg.header.segmentID
		if seg.footer.nextSequence > nextSequence {
			nextSequence = seg.footer.nextSequence
		}
	}
	if err := s.journal.EnsureNextSequence(maxTapeLogicalSequence + 1); err != nil {
		return fmt.Errorf("advancing journal sequence: %w", err)
	}
	snap, err := s.journal.Snapshot()
	if err != nil {
		return fmt.Errorf("scanning journal: %w", err)
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

	s.migrator = NewMigrator(s.journal, s.device, s.recordSize, s.policy, previousSegment, nextSequence, capturedOps)

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
	return sealUntrustedTail(ctx, s.device, tailBlock)
}

func (s *tapePartStore) Stop(ctx context.Context) error {
	_, span := s.tracer.Start(ctx, "tapePartStore.Stop")
	defer span.End()

	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	if s.workerCancel != nil {
		s.workerCancel()
	}
	if s.workerDone != nil {
		select {
		case <-s.workerDone:
		case <-ctx.Done():
			return fmt.Errorf("stopping tape migration worker: %w", ctx.Err())
		}
	}

	s.mu.Lock()
	scheduler := s.scheduler
	s.scheduler = nil
	s.mu.Unlock()
	if scheduler != nil {
		if err := scheduler.stop(ctx); err != nil {
			return err
		}
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

func (s *tapePartStore) OperationalStatus() (OperationalStatus, error) {
	s.mu.Lock()
	if err := s.checkStarted(); err != nil {
		s.mu.Unlock()
		return OperationalStatus{}, err
	}
	status := OperationalStatus{
		VolumeID:           s.catalog.VolumeID,
		MediaID:            fmt.Sprintf("%x", s.catalog.MediaID),
		Segments:           len(s.catalog.Segments),
		LiveParts:          len(s.index),
		ReadCacheEntries:   len(s.cacheEntries),
		ReadCacheBytes:     s.cacheBytes,
		EndOfMedia:         s.endOfMedia,
		LastMigrationError: s.lastMigrationError,
	}
	j := s.journal
	scheduler := s.scheduler
	s.mu.Unlock()

	if scheduler != nil {
		status.DriveQueueDepth, status.CurrentTapeBlock = scheduler.status()
	}
	snapshot, err := j.Snapshot()
	if err != nil {
		return OperationalStatus{}, err
	}
	for generation, part := range snapshot.Parts {
		if part.Checkpointed {
			continue
		}
		if liveGeneration, live := snapshot.Live[part.PartID]; !live || liveGeneration != generation {
			continue
		}
		status.JournalBacklogParts++
		status.JournalBacklogBytes += part.Length
	}
	return status, nil
}

func (s *tapePartStore) FinalizeObjectLayout(ctx context.Context, tx database.Tx, layout partstore.ObjectLayout) error {
	s.mu.Lock()
	if err := s.checkStarted(); err != nil {
		s.mu.Unlock()
		return err
	}
	j := s.journal
	s.mu.Unlock()
	if tx == nil {
		if err := j.FinalizeObjectLayout(ctx, layout.ObjectID, layout.PartIDs); err != nil {
			return err
		}
		s.triggerMigration()
		return nil
	}
	sequence, err := j.StageObjectLayout(layout.ObjectID, layout.PartIDs)
	if err != nil {
		return err
	}
	tx.OnPreCommit(func(context.Context) error {
		return j.EnsureDurable(sequence)
	})
	tx.OnAfterCommit(func(context.Context) error {
		s.triggerMigration()
		return nil
	})
	return nil
}

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
	loc, err := j.StagePart(ctx, input, reader)
	if err != nil {
		return fmt.Errorf("staging part %s to journal: %w", partId.String(), err)
	}
	newEntry := indexEntry{generation: gen, length: loc.Length, hash: loc.Hash, location: locationJournal, journalLoc: loc}

	if tx == nil {
		unlock := s.partLocks.Lock(partId)
		defer unlock()

		s.mu.Lock()
		var prevGen *journal.GenerationID
		if entry, ok := s.index[partId]; ok {
			g := entry.generation
			prevGen = &g
		}
		s.mu.Unlock()
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
	var prevGen *journal.GenerationID
	var unlock func()
	tx.OnPreCommit(func(ctx context.Context) error {
		unlock = s.partLocks.Lock(partId)
		s.mu.Lock()
		if entry, ok := s.index[partId]; ok {
			g := entry.generation
			prevGen = &g
		}
		s.mu.Unlock()

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
		defer unlock()
		s.mu.Lock()
		s.index[partId] = newEntry
		s.mu.Unlock()
		s.triggerMigration()
		return nil
	})
	tx.OnRollback(func(ctx context.Context) error {
		if unlock != nil {
			defer unlock()
		}
		if !activated {
			j.AbandonGeneration(gen)
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
	return s.openPartAtEntry(ctx, partId, entry, j)
}

func (s *tapePartStore) openPartAtEntry(ctx context.Context, partId partstore.PartId, entry indexEntry, j *journal.Journal) (io.ReadCloser, error) {
	if entry.location == locationJournal {
		reader, err := j.OpenPayload(entry.journalLoc)
		if err == nil {
			return reader, nil
		}

		// Migration may have published the durable tape location and compacted
		// the old journal file after GetPart copied entry. Retry from tape only
		// when the same generation has made that transition.
		s.mu.Lock()
		refreshed, ok := s.index[partId]
		s.mu.Unlock()
		if !ok || refreshed.generation != entry.generation || refreshed.location != locationTape {
			return nil, err
		}
		entry = refreshed
	}
	return s.openCachedTapePart(ctx, partId, entry)
}

func (s *tapePartStore) newTapePartReader(ctx context.Context, device tapedev.Device, partId partstore.PartId, entry indexEntry) *tapePartReader {
	return &tapePartReader{
		device:             device,
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
		unlock := s.partLocks.Lock(partId)
		defer unlock()

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
		s.triggerMigration()
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

// runMigration drains every currently-ready segment before sleeping again.
func (s *tapePartStore) runMigration(ctx context.Context, force bool) {
	s.mu.Lock()
	migrator := s.migrator
	scheduler := s.scheduler
	s.mu.Unlock()
	if migrator == nil || scheduler == nil {
		return
	}
	for {
		value, err := scheduler.migration(ctx, func(jobCtx context.Context, _ tapedev.Device) (any, error) {
			return migrator.MigrateOnce(jobCtx, force)
		})
		if err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return
			}
			s.mu.Lock()
			s.lastMigrationError = err.Error()
			s.endOfMedia = errors.Is(err, tapedev.ErrEndOfTape)
			s.mu.Unlock()
			slog.WarnContext(ctx, "Tape migration failed", "error", err)
			return
		}
		res, ok := value.(MigrationResult)
		if !ok {
			slog.WarnContext(ctx, "Tape migration returned an invalid result")
			return
		}
		if !res.Committed {
			return
		}
		s.mu.Lock()
		s.lastMigrationError = ""
		s.endOfMedia = false
		s.catalog.appendSegment(res.segment)
		catalogErr := saveTapeCatalog(s.journalDir, s.catalog)
		s.mu.Unlock()
		if catalogErr != nil {
			slog.WarnContext(ctx, "Tape migration catalog checkpoint failed", "error", catalogErr)
			return
		}
		// The catalog is durable after the tape segment. Only now may journal
		// payloads become reclaimable.
		checkpoints := make([]journal.Checkpoint, 0, len(res.Parts))
		for _, p := range res.Parts {
			checkpoints = append(checkpoints, journal.Checkpoint{Generation: p.Generation, SegmentID: res.SegmentID})
		}
		if _, err := s.journal.CheckpointBatch(ctx, checkpoints); err != nil {
			slog.WarnContext(ctx, "Tape migration journal checkpoint batch failed", "parts", len(checkpoints), "error", err)
			return
		}
		// Publish the durable tape locations before compaction can unlink their
		// journal files. GetPart must never observe a journal locator after its
		// backing file has become reclaimable.
		s.mu.Lock()
		for _, p := range res.Parts {
			if entry, ok := s.index[p.PartID]; ok && entry.generation == p.Generation {
				entry.location = locationTape
				entry.tapeBlock = p.StartBlock
				s.index[p.PartID] = entry
			}
		}
		s.mu.Unlock()
		if err := s.journal.Compact(ctx); err != nil {
			// Reclamation is an optimization; catalog + journal checkpoints are
			// already durable, so keep serving and retry on the next migration.
			slog.WarnContext(ctx, "Tape journal compaction failed", "error", err)
		}
	}
}

// tapePartReader streams one migrated part while its enclosing scheduler job
// owns the device exclusively.
type tapePartReader struct {
	device             tapedev.Device
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
	if err := r.positionLocked(); err != nil {
		return err
	}
	if !r.headerVerified {
		n, err := r.device.ReadRecord(r.ctx, r.buf)
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
	n, err := r.device.ReadRecord(r.ctx, r.buf)
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
	pos, err := r.device.Tell(r.ctx)
	if err != nil {
		return err
	}
	if pos.Block == r.nextBlock {
		return nil
	}
	if !r.headerVerified && pos.Block+1 == r.nextBlock {
		n, err := r.device.ReadRecord(r.ctx, r.buf)
		if err != nil {
			return err
		}
		rec, err := decodeSegmentRecord(r.buf[:n])
		if err != nil || rec.kind != segKindPartCommit {
			return fmt.Errorf("tape part %s: expected adjacent part commit at block %d", r.partId.String(), pos.Block)
		}
		return nil
	}
	return r.device.LocateBlock(r.ctx, r.nextBlock)
}

func (r *tapePartReader) Close() error {
	r.closed = true
	return nil
}
