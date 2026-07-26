// Package journal implements the durable disk staging layer of the tape part
// store. Part payloads and the logical operations over them (activation,
// deletion, tape-migration checkpoints) are appended to crash-safe,
// append-only segment files before anything is written to tape. Correctness
// is reconstructable from the journal alone: a part is durable only once its
// begin/data/end/commit records are all present and verified, and the live
// set is computed by replaying activation and deletion records in sequence
// order.
package journal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// DurabilityMode selects how PutPart-completing appends reach the disk.
type DurabilityMode int

const (
	// DurabilityPerPart fsyncs after every part and logical operation.
	DurabilityPerPart DurabilityMode = iota
	// DurabilityGroupCommit batches concurrent appends behind a shared fsync.
	DurabilityGroupCommit
)

// GroupCommitPolicy bounds a group-commit batch window.
type GroupCommitPolicy struct {
	MaxDelay time.Duration
	MaxBytes int64
}

const (
	defaultMaxFileBytes int64 = 1 << 30 // 1 GiB per journal segment file
	dataChunkSize             = 1 << 20 // 1 MiB part-data records
	minCompactionBytes  int64 = 64 << 20
	filePrefix                = "journal-"
	fileSuffix                = ".pj"
)

type Options struct {
	Dir          string
	Durability   DurabilityMode
	GroupCommit  GroupCommitPolicy
	MaxFileBytes int64
}

// PartInput describes a part to stage, including advisory placement hints.
type PartInput struct {
	Generation GenerationID
	PartID     partstore.PartId
	ObjectID   *partstore.ObjectId
	PartNumber *uint64
	PartCount  *uint64
	ObjectSize *uint64
}

func (in PartInput) toPayload() partBeginPayload {
	return partBeginPayload{
		generation: in.Generation,
		partID:     in.PartID,
		objectID:   in.ObjectID,
		partNumber: in.PartNumber,
		partCount:  in.PartCount,
		objectSize: in.ObjectSize,
	}
}

// Locator points at a committed part's data records within a journal file.
type Locator struct {
	FileIndex     uint64
	DataOffset    int64
	DataEndOffset int64
	Length        uint64
	Hash          [32]byte
}

type Journal struct {
	dir          string
	durability   DurabilityMode
	groupPolicy  GroupCommitPolicy
	maxFileBytes int64
	journalID    [16]byte

	mutationMu   sync.RWMutex
	appendMu     sync.Mutex
	file         *os.File
	fileIndex    uint64
	fileOffset   int64
	nextSeq      uint64
	writtenSeq   atomic.Uint64
	pendingBytes atomic.Int64

	syncMu    sync.Mutex
	syncCond  *sync.Cond
	syncing   bool
	syncedSeq uint64
	syncErr   error
	syncWake  chan struct{}

	stateMu sync.RWMutex
	state   *RecoveryResult
	staged  map[GenerationID]stagedPart
	layouts []stagedObjectLayout
	pending map[GenerationID]uint64

	closed bool
}

type stagedPart struct {
	part      *RecoveredPart
	commitSeq uint64
}

type stagedObjectLayout struct {
	sequence uint64
	payload  objectLayoutPayload
}

// Open opens (creating if necessary) a journal directory. Existing segment
// files are scanned so appends continue after the last durable record.
func Open(opts Options) (*Journal, error) {
	if opts.Dir == "" {
		return nil, errors.New("journal: Dir is required")
	}
	if opts.Durability != DurabilityPerPart && opts.Durability != DurabilityGroupCommit {
		return nil, fmt.Errorf("journal: invalid durability mode %d", opts.Durability)
	}
	if opts.GroupCommit.MaxDelay < 0 || opts.GroupCommit.MaxBytes < 0 {
		return nil, errors.New("journal: group commit delay and bytes must be non-negative")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	maxFileBytes := opts.MaxFileBytes
	if maxFileBytes <= 0 {
		maxFileBytes = defaultMaxFileBytes
	}
	j := &Journal{
		dir:          opts.Dir,
		durability:   opts.Durability,
		groupPolicy:  opts.GroupCommit,
		maxFileBytes: maxFileBytes,
	}
	j.syncCond = sync.NewCond(&j.syncMu)
	j.syncWake = make(chan struct{}, 1)
	j.staged = make(map[GenerationID]stagedPart)
	j.pending = make(map[GenerationID]uint64)

	scan, err := Scan(opts.Dir)
	if err != nil {
		return nil, err
	}
	j.nextSeq = scan.NextSequence
	j.state = scan
	if scan.JournalID != ([16]byte{}) {
		j.journalID = scan.JournalID
	} else {
		if _, err := rand.Read(j.journalID[:]); err != nil {
			return nil, err
		}
	}
	// Continue appending to a fresh segment file after the highest existing
	// index, so recovery never has to reason about a partially rolled file.
	j.fileIndex = scan.MaxFileIndex + 1
	if err := j.openNewFile(); err != nil {
		return nil, err
	}
	j.writtenSeq.Store(j.nextSeq - 1)
	j.syncedSeq = j.nextSeq - 1
	return j, nil
}

func fileName(index uint64) string {
	return fmt.Sprintf("%s%020d%s", filePrefix, index, fileSuffix)
}

func (j *Journal) openNewFile() error {
	path := filepath.Join(j.dir, fileName(j.fileIndex))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	j.file = f
	j.fileOffset = 0
	hdr := encodeJournalHeader(journalHeaderPayload{
		journalID:       j.journalID,
		createdUnixNano: time.Now().UnixNano(),
		fileIndex:       j.fileIndex,
	})
	if err := j.appendRecordLocked(kindJournalHeader, 0, hdr); err != nil {
		_ = f.Close()
		return err
	}
	return nil
}

// appendRecordLocked encodes and writes one record, assigning it the next
// sequence. Must hold appendMu.
func (j *Journal) appendRecordLocked(kind uint8, flags uint16, payload []byte) error {
	seq := j.nextSeq
	rec, err := encodeRecord(kind, flags, seq, payload)
	if err != nil {
		return err
	}
	if _, err := j.file.Write(rec); err != nil {
		return err
	}
	j.fileOffset += int64(len(rec))
	pending := j.pendingBytes.Add(int64(len(rec)))
	j.nextSeq++
	j.writtenSeq.Store(seq)
	if j.durability == DurabilityGroupCommit && j.groupPolicy.MaxBytes > 0 && pending >= j.groupPolicy.MaxBytes {
		select {
		case j.syncWake <- struct{}{}:
		default:
		}
	}
	return nil
}

// maybeRollLocked starts a new segment file when the current one is over the
// size limit. It is only called between parts so a part's records never span
// files. Must hold appendMu.
func (j *Journal) maybeRollLocked() error {
	if j.fileOffset < j.maxFileBytes {
		return nil
	}
	if err := j.file.Sync(); err != nil {
		return err
	}
	if err := j.file.Close(); err != nil {
		return err
	}
	// Everything written so far is now durable.
	j.syncMu.Lock()
	if w := j.writtenSeq.Load(); w > j.syncedSeq {
		j.syncedSeq = w
	}
	j.syncMu.Unlock()
	j.fileIndex++
	return j.openNewFile()
}

func (j *Journal) checkOpen() error {
	if j.closed {
		return errors.New("journal: closed")
	}
	return nil
}

// WritePart writes and immediately makes one part durable. TapePartStore uses
// StagePart instead so the later activation can make both payload and logical
// commit durable with one sync.
func (j *Journal) WritePart(ctx context.Context, input PartInput, reader io.Reader) (Locator, error) {
	return j.writePart(ctx, input, reader, true)
}

// StagePart appends a complete part without forcing a sync. The next durable
// journal operation (normally Activate in the database pre-commit hook) makes
// both the payload and activation durable together.
func (j *Journal) StagePart(ctx context.Context, input PartInput, reader io.Reader) (Locator, error) {
	return j.writePart(ctx, input, reader, false)
}

func (j *Journal) writePart(ctx context.Context, input PartInput, reader io.Reader, syncNow bool) (Locator, error) {
	j.mutationMu.RLock()
	defer j.mutationMu.RUnlock()
	begin := input.toPayload()
	j.appendMu.Lock()
	if err := j.checkOpen(); err != nil {
		j.appendMu.Unlock()
		return Locator{}, err
	}
	if err := j.maybeRollLocked(); err != nil {
		j.appendMu.Unlock()
		return Locator{}, err
	}
	fileIndex := j.fileIndex
	if err := j.appendRecordLocked(kindPartBegin, 0, encodePartBegin(begin)); err != nil {
		j.appendMu.Unlock()
		return Locator{}, err
	}
	dataOffset := j.fileOffset
	hasher := sha256.New()
	var length uint64
	buf := make([]byte, dataChunkSize)
	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			hasher.Write(buf[:n])
			length += uint64(n)
			if err := j.appendRecordLocked(kindPartData, 0, buf[:n]); err != nil {
				j.appendMu.Unlock()
				return Locator{}, err
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			j.appendMu.Unlock()
			return Locator{}, fmt.Errorf("journal: reading part content: %w", readErr)
		}
	}
	dataEndOffset := j.fileOffset
	var hash [32]byte
	copy(hash[:], hasher.Sum(nil))
	end := partEndPayload{generation: input.Generation, dataLength: length, dataHash: hash}
	if err := j.appendRecordLocked(kindPartEnd, 0, encodePartEnd(end)); err != nil {
		j.appendMu.Unlock()
		return Locator{}, err
	}
	if err := j.appendRecordLocked(kindPartCommit, 0, encodeGeneration(input.Generation)); err != nil {
		j.appendMu.Unlock()
		return Locator{}, err
	}
	commitSeq := j.nextSeq - 1
	j.appendMu.Unlock()

	locator := Locator{
		FileIndex:     fileIndex,
		DataOffset:    dataOffset,
		DataEndOffset: dataEndOffset,
		Length:        length,
		Hash:          hash,
	}
	j.stateMu.Lock()
	j.staged[input.Generation] = stagedPart{part: &RecoveredPart{
		Generation: input.Generation,
		PartID:     input.PartID,
		Location:   locator,
		ObjectID:   input.ObjectID,
		PartNumber: input.PartNumber,
		PartCount:  input.PartCount,
		Length:     length,
		Hash:       hash,
	}, commitSeq: commitSeq}
	j.pending[input.Generation] = fileIndex
	j.stateMu.Unlock()
	if syncNow {
		if err := j.durable(commitSeq); err != nil {
			return Locator{}, err
		}
	}
	return locator, nil
}

func (j *Journal) appendLogical(kind uint8, payload []byte) (uint64, error) {
	j.appendMu.Lock()
	if err := j.checkOpen(); err != nil {
		j.appendMu.Unlock()
		return 0, err
	}
	if err := j.appendRecordLocked(kind, 0, payload); err != nil {
		j.appendMu.Unlock()
		return 0, err
	}
	seq := j.nextSeq - 1
	j.appendMu.Unlock()
	if err := j.durable(seq); err != nil {
		return 0, err
	}
	return seq, nil
}

// Activate records that partID's live generation is now generation, optionally
// asserting the currently-active generation. Returns the record's sequence
// (its position in the global logical order).
func (j *Journal) Activate(ctx context.Context, partID partstore.PartId, generation GenerationID, expectedPrevious *GenerationID) (uint64, error) {
	j.mutationMu.RLock()
	defer j.mutationMu.RUnlock()
	seq, err := j.appendLogical(kindActivate, encodeActivate(activatePayload{
		partID:           partID,
		generation:       generation,
		expectedPrevious: expectedPrevious,
	}))
	if err != nil {
		return 0, err
	}
	j.stateMu.Lock()
	j.state.Ops = append(j.state.Ops, LogicalOp{
		Sequence:         seq,
		Kind:             kindActivate,
		PartID:           partID,
		Generation:       generation,
		ExpectedPrevious: cloneGeneration(expectedPrevious),
	})
	j.state.NextSequence = max(j.state.NextSequence, seq+1)
	delete(j.pending, generation)
	j.stateMu.Unlock()
	return seq, nil
}

// Delete records a logical deletion of partID, optionally asserting which
// generation is being deleted.
func (j *Journal) Delete(ctx context.Context, partID partstore.PartId, expectedGeneration *GenerationID) (uint64, error) {
	j.mutationMu.RLock()
	defer j.mutationMu.RUnlock()
	seq, err := j.appendLogical(kindDelete, encodeDelete(deletePayload{
		partID:             partID,
		expectedGeneration: expectedGeneration,
	}))
	if err != nil {
		return 0, err
	}
	j.stateMu.Lock()
	j.state.Ops = append(j.state.Ops, LogicalOp{
		Sequence:           seq,
		Kind:               kindDelete,
		PartID:             partID,
		ExpectedGeneration: cloneGeneration(expectedGeneration),
	})
	j.state.NextSequence = max(j.state.NextSequence, seq+1)
	j.stateMu.Unlock()
	return seq, nil
}

// Checkpoint records that generation's payload is now durable on a committed
// tape segment, making the journal copy reclaimable during compaction.
func (j *Journal) Checkpoint(ctx context.Context, generation GenerationID, segmentID [16]byte) (uint64, error) {
	sequences, err := j.CheckpointBatch(ctx, []Checkpoint{{Generation: generation, SegmentID: segmentID}})
	if err != nil {
		return 0, err
	}
	return sequences[0], nil
}

// Checkpoint is one generation-to-segment durability transition.
type Checkpoint struct {
	Generation GenerationID
	SegmentID  [16]byte
}

// CheckpointBatch appends all checkpoint records and makes them durable with a
// single sync. Segment migration must use this method: per-part syncs destroy
// throughput when a segment contains thousands of small parts.
func (j *Journal) CheckpointBatch(ctx context.Context, checkpoints []Checkpoint) ([]uint64, error) {
	if len(checkpoints) == 0 {
		return nil, nil
	}
	j.mutationMu.RLock()
	defer j.mutationMu.RUnlock()
	j.appendMu.Lock()
	if err := j.checkOpen(); err != nil {
		j.appendMu.Unlock()
		return nil, err
	}
	if err := j.maybeRollLocked(); err != nil {
		j.appendMu.Unlock()
		return nil, err
	}
	sequences := make([]uint64, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if err := j.appendRecordLocked(kindCheckpoint, 0, encodeCheckpoint(checkpointPayload{
			generation: checkpoint.Generation,
			segmentID:  checkpoint.SegmentID,
		})); err != nil {
			j.appendMu.Unlock()
			return nil, err
		}
		sequences = append(sequences, j.nextSeq-1)
	}
	lastSequence := sequences[len(sequences)-1]
	j.appendMu.Unlock()
	if err := j.durable(lastSequence); err != nil {
		return nil, err
	}

	j.stateMu.Lock()
	for _, checkpoint := range checkpoints {
		if part := j.state.Parts[checkpoint.Generation]; part != nil {
			part.Checkpointed = true
			part.CheckpointSegment = checkpoint.SegmentID
		}
	}
	j.state.NextSequence = max(j.state.NextSequence, lastSequence+1)
	j.stateMu.Unlock()
	return sequences, nil
}

// durable makes all records up to and including seq durable on disk.
func (j *Journal) durable(seq uint64) error {
	var err error
	if j.durability == DurabilityPerPart {
		j.syncMu.Lock()
		if j.syncedSeq >= seq {
			j.syncMu.Unlock()
			j.publishDurableStaged()
			return nil
		}
		j.syncMu.Unlock()
		err = j.fsyncAndAdvance()
	} else {
		err = j.waitDurable(seq)
	}
	if err == nil {
		j.publishDurableStaged()
	}
	return err
}

func (j *Journal) publishDurableStaged() {
	j.syncMu.Lock()
	syncedSeq := j.syncedSeq
	j.syncMu.Unlock()
	j.stateMu.Lock()
	sort.Slice(j.layouts, func(i, k int) bool { return j.layouts[i].sequence < j.layouts[k].sequence })
	remainingLayouts := j.layouts[:0]
	for _, staged := range j.layouts {
		if staged.sequence > syncedSeq {
			remainingLayouts = append(remainingLayouts, staged)
			continue
		}
		applyObjectLayout(j.state, staged.payload)
		j.state.NextSequence = max(j.state.NextSequence, staged.sequence+1)
	}
	j.layouts = remainingLayouts
	for generation, staged := range j.staged {
		if staged.commitSeq > syncedSeq {
			continue
		}
		applyRecoveredPlacement(j.state, staged.part)
		j.state.Parts[generation] = staged.part
		j.state.partsByID[staged.part.PartID] = append(j.state.partsByID[staged.part.PartID], generation)
		j.state.NextSequence = max(j.state.NextSequence, staged.commitSeq+1)
		j.state.MaxFileIndex = max(j.state.MaxFileIndex, staged.part.Location.FileIndex)
		delete(j.staged, generation)
	}
	j.stateMu.Unlock()
}

// FinalizeObjectLayout durably records the completed ordered part manifest and
// enriches pending parts with the now-known PartCount. This is advisory
// placement metadata: replaying it before or after a database rollback cannot
// change logical part visibility.
func (j *Journal) FinalizeObjectLayout(ctx context.Context, objectID partstore.ObjectId, partIDs []partstore.PartId) error {
	j.mutationMu.RLock()
	defer j.mutationMu.RUnlock()
	payload, err := validateObjectLayout(objectID, partIDs)
	if err != nil {
		return err
	}
	seq, err := j.appendLogical(kindObjectLayout, encodeObjectLayout(payload))
	if err != nil {
		return err
	}
	j.stateMu.Lock()
	applyObjectLayout(j.state, payload)
	j.state.NextSequence = max(j.state.NextSequence, seq+1)
	j.stateMu.Unlock()
	return nil
}

// StageObjectLayout appends placement metadata without forcing a sync. The
// caller must call EnsureDurable before its database transaction commits.
// When a part activation is already registered as an earlier pre-commit hook,
// that activation's single sync covers payload, layout, and activation.
func (j *Journal) StageObjectLayout(objectID partstore.ObjectId, partIDs []partstore.PartId) (uint64, error) {
	j.mutationMu.RLock()
	defer j.mutationMu.RUnlock()
	payload, err := validateObjectLayout(objectID, partIDs)
	if err != nil {
		return 0, err
	}
	j.appendMu.Lock()
	if err := j.checkOpen(); err != nil {
		j.appendMu.Unlock()
		return 0, err
	}
	if err := j.appendRecordLocked(kindObjectLayout, 0, encodeObjectLayout(payload)); err != nil {
		j.appendMu.Unlock()
		return 0, err
	}
	sequence := j.nextSeq - 1
	j.appendMu.Unlock()
	j.stateMu.Lock()
	j.layouts = append(j.layouts, stagedObjectLayout{sequence: sequence, payload: payload})
	j.stateMu.Unlock()
	return sequence, nil
}

// EnsureDurable makes every journal record through sequence durable.
func (j *Journal) EnsureDurable(sequence uint64) error {
	j.mutationMu.RLock()
	defer j.mutationMu.RUnlock()
	return j.durable(sequence)
}

// AbandonGeneration releases runtime compaction protection for a staged
// generation whose database transaction rolled back before activation. Its
// append-only bytes remain harmless and become reclaimable.
func (j *Journal) AbandonGeneration(generation GenerationID) {
	j.stateMu.Lock()
	delete(j.pending, generation)
	delete(j.staged, generation)
	j.stateMu.Unlock()
}

func validateObjectLayout(objectID partstore.ObjectId, partIDs []partstore.PartId) (objectLayoutPayload, error) {
	if len(partIDs) == 0 || len(partIDs) > 10_000 {
		return objectLayoutPayload{}, fmt.Errorf("journal: object layout must contain 1..10000 parts")
	}
	return objectLayoutPayload{objectID: objectID, partIDs: append([]partstore.PartId(nil), partIDs...)}, nil
}

func applyObjectLayout(state *RecoveryResult, payload objectLayoutPayload) {
	objectID := payload.objectID
	partIDs := payload.partIDs
	partCount := uint64(len(partIDs))
	seen := make(map[partstore.PartId]struct{}, len(partIDs))
	for index, partID := range partIDs {
		// Content deduplication can legitimately make multiple object positions
		// reference one physical part. One payload cannot occupy two tape
		// positions, so retain its first logical position while preserving the
		// full PartCount.
		if _, duplicate := seen[partID]; duplicate {
			continue
		}
		seen[partID] = struct{}{}
		partNumber := uint64(index + 1)
		state.layouts[partID] = recoveredPlacement{objectID: objectID, partNumber: partNumber, partCount: partCount}
		for _, generation := range state.partsByID[partID] {
			part := state.Parts[generation]
			if part == nil {
				continue
			}
			id := objectID
			part.ObjectID = &id
			part.PartNumber = &partNumber
			part.PartCount = &partCount
		}
	}
}

// fsyncAndAdvance fsyncs the current file and advances syncedSeq to the
// highest written sequence at the moment of the sync.
func (j *Journal) fsyncAndAdvance() error {
	if j.durability == DurabilityGroupCommit {
		j.pendingBytes.Store(0)
	}
	target := j.writtenSeq.Load()
	err := j.file.Sync()
	j.syncMu.Lock()
	defer j.syncMu.Unlock()
	if err != nil {
		j.syncErr = err
		return err
	}
	if target > j.syncedSeq {
		j.syncedSeq = target
	}
	return nil
}

func (j *Journal) waitDurable(seq uint64) error {
	j.syncMu.Lock()
	defer j.syncMu.Unlock()
	for {
		if j.syncErr != nil {
			return j.syncErr
		}
		if j.syncedSeq >= seq {
			return nil
		}
		if !j.syncing {
			j.syncing = true
			if j.groupPolicy.MaxDelay > 0 {
				j.syncMu.Unlock()
				timer := time.NewTimer(j.groupPolicy.MaxDelay)
				select {
				case <-timer.C:
				case <-j.syncWake:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
				}
				j.syncMu.Lock()
			}
			// Clear the completed batch before taking its sequence boundary.
			// Appends racing after this point accumulate toward the next
			// MaxBytes wake instead of being erased after Sync returns.
			j.pendingBytes.Store(0)
			target := j.writtenSeq.Load()
			j.syncMu.Unlock()
			err := j.file.Sync()
			j.syncMu.Lock()
			j.syncing = false
			if err != nil {
				j.syncErr = err
			} else if target > j.syncedSeq {
				j.syncedSeq = target
			}
			j.syncCond.Broadcast()
			continue
		}
		j.syncCond.Wait()
	}
}

// OpenPayload returns a reader over a committed part's data. It reads the data
// records in loc's byte range from the on-disk file, verifying each record.
func (j *Journal) OpenPayload(loc Locator) (io.ReadCloser, error) {
	path := filepath.Join(j.dir, fileName(loc.FileIndex))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &payloadReader{f: f, offset: loc.DataOffset, end: loc.DataEndOffset}, nil
}

func (j *Journal) Close() error {
	j.mutationMu.Lock()
	defer j.mutationMu.Unlock()
	j.appendMu.Lock()
	defer j.appendMu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	if j.file == nil {
		return nil
	}
	if err := j.file.Sync(); err != nil {
		_ = j.file.Close()
		return err
	}
	return j.file.Close()
}

// Compact reclaims closed journal files whose payloads are either safely
// checkpointed on tape or no longer live. Before deleting anything it writes
// a durable, unconditional activation snapshot for the current live set, so
// logical state remains reconstructable after the historical files disappear.
func (j *Journal) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	j.mutationMu.Lock()
	defer j.mutationMu.Unlock()
	j.appendMu.Lock()
	defer j.appendMu.Unlock()
	if err := j.checkOpen(); err != nil {
		return err
	}

	// mutationMu guarantees all earlier durable operations have published their
	// incremental state and prevents a database transaction's staged payload
	// from racing reclamation.
	if err := j.fsyncAndAdvance(); err != nil {
		return err
	}
	j.publishDurableStaged()
	j.stateMu.RLock()
	snapshot := cloneRecoveryResult(j.state)
	protectedFiles := make(map[uint64]struct{}, len(j.pending))
	for _, fileIndex := range j.pending {
		protectedFiles[fileIndex] = struct{}{}
	}
	j.stateMu.RUnlock()
	rebuildLive(snapshot)
	// Rotate a sizable active file when all of its live payloads have reached
	// tape. This caps retained staging data even when ingestion becomes idle
	// immediately after one large migration.
	if j.fileOffset >= minCompactionBytes && fileReclaimable(snapshot, j.fileIndex, protectedFiles) {
		if err := j.file.Sync(); err != nil {
			return err
		}
		if err := j.file.Close(); err != nil {
			return err
		}
		j.fileIndex++
		if err := j.openNewFile(); err != nil {
			return err
		}
	}

	indices, err := sortedFileIndices(j.dir)
	if err != nil {
		return err
	}
	var reclaim []uint64
	for _, index := range indices {
		if index < j.fileIndex && fileReclaimable(snapshot, index, protectedFiles) {
			reclaim = append(reclaim, index)
		}
	}
	if len(reclaim) == 0 {
		return nil
	}

	// Snapshot the complete logical state in the surviving active file.
	type layoutPart struct {
		id     partstore.PartId
		number uint64
	}
	layouts := make(map[partstore.ObjectId][]layoutPart)
	for partID, placement := range snapshot.layouts {
		layouts[placement.objectID] = append(layouts[placement.objectID], layoutPart{id: partID, number: placement.partNumber})
	}
	for objectID, parts := range layouts {
		sort.Slice(parts, func(i, k int) bool { return parts[i].number < parts[k].number })
		partIDs := make([]partstore.PartId, len(parts))
		for index, part := range parts {
			partIDs[index] = part.id
		}
		if err := j.appendRecordLocked(kindObjectLayout, 0, encodeObjectLayout(objectLayoutPayload{
			objectID: objectID,
			partIDs:  partIDs,
		})); err != nil {
			return err
		}
	}
	for partID, generation := range snapshot.Live {
		if err := j.appendRecordLocked(kindActivate, 0, encodeActivate(activatePayload{
			partID:     partID,
			generation: generation,
		})); err != nil {
			return err
		}
	}
	knownParts := make(map[partstore.PartId]struct{})
	for _, op := range snapshot.Ops {
		knownParts[op.PartID] = struct{}{}
	}
	for partID := range knownParts {
		if _, live := snapshot.Live[partID]; live {
			continue
		}
		if err := j.appendRecordLocked(kindDelete, 0, encodeDelete(deletePayload{
			partID: partID,
		})); err != nil {
			return err
		}
	}
	if err := j.fsyncAndAdvance(); err != nil {
		return err
	}

	for _, index := range reclaim {
		if err := os.Remove(filepath.Join(j.dir, fileName(index))); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := ioutils.SyncDirectory(j.dir); err != nil {
		return err
	}

	rebuilt, err := Scan(j.dir)
	if err != nil {
		return err
	}
	j.stateMu.Lock()
	j.state = rebuilt
	j.stateMu.Unlock()
	return nil
}

func fileReclaimable(snapshot *RecoveryResult, fileIndex uint64, protectedFiles map[uint64]struct{}) bool {
	if _, protected := protectedFiles[fileIndex]; protected {
		return false
	}
	live := make(map[GenerationID]struct{}, len(snapshot.Live))
	for _, generation := range snapshot.Live {
		live[generation] = struct{}{}
	}
	for generation, part := range snapshot.Parts {
		if part.Location.FileIndex != fileIndex {
			continue
		}
		if _, isLive := live[generation]; isLive && !part.Checkpointed {
			return false
		}
	}
	return true
}

// JournalID returns the identifier shared by this journal's segment files.
func (j *Journal) JournalID() [16]byte { return j.journalID }

// EnsureNextSequence advances the journal's sequence namespace without
// writing a record. Tape recovery uses it before serving requests when the
// disk journal was rebuilt, preventing new logical operations from colliding
// with sequence numbers already preserved on tape.
func (j *Journal) EnsureNextSequence(minimum uint64) error {
	j.appendMu.Lock()
	defer j.appendMu.Unlock()
	if err := j.checkOpen(); err != nil {
		return err
	}
	if minimum > j.nextSeq {
		j.nextSeq = minimum
	}
	return nil
}

// Snapshot returns a consistent clone of the incrementally maintained durable
// state. The journal is scanned only once during Open; migration therefore
// scales with pending metadata rather than all payload bytes ever staged.
func (j *Journal) Snapshot() (*RecoveryResult, error) {
	j.stateMu.RLock()
	snapshot := cloneRecoveryResult(j.state)
	j.stateMu.RUnlock()
	rebuildLive(snapshot)
	return snapshot, nil
}

func cloneGeneration(g *GenerationID) *GenerationID {
	if g == nil {
		return nil
	}
	copy := *g
	return &copy
}

type payloadReader struct {
	f       *os.File
	offset  int64
	end     int64
	pending []byte
}

func (r *payloadReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.offset >= r.end {
			return 0, io.EOF
		}
		hdrBuf := make([]byte, envelopeHeaderSize)
		if _, err := readAtFull(r.f, hdrBuf, r.offset); err != nil {
			return 0, err
		}
		h, err := decodeHeader(hdrBuf)
		if err != nil {
			return 0, err
		}
		if h.kind != kindPartData {
			return 0, fmt.Errorf("journal: expected part-data record, got kind %d", h.kind)
		}
		rec := make([]byte, h.payloadSize)
		if _, err := readAtFull(r.f, rec, r.offset); err != nil {
			return 0, err
		}
		payload, err := verifyPayload(rec, h)
		if err != nil {
			return 0, err
		}
		r.pending = payload
		r.offset += int64(h.payloadSize)
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (r *payloadReader) Close() error { return r.f.Close() }

func readAtFull(f *os.File, buf []byte, off int64) (int, error) {
	n, err := f.ReadAt(buf, off)
	if err != nil && n == len(buf) {
		return n, nil
	}
	return n, err
}

// sortedFileIndices lists journal segment file indices in ascending order.
func sortedFileIndices(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var indices []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) != len(filePrefix)+20+len(fileSuffix) {
			continue
		}
		var idx uint64
		if _, err := fmt.Sscanf(name, filePrefix+"%020d"+fileSuffix, &idx); err != nil {
			continue
		}
		indices = append(indices, idx)
	}
	slices.Sort(indices)
	return indices, nil
}
