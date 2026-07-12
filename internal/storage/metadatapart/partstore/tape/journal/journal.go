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
	"sync"
	"sync/atomic"
	"time"

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

	appendMu   sync.Mutex
	file       *os.File
	fileIndex  uint64
	fileOffset int64
	nextSeq    uint64
	writtenSeq atomic.Uint64

	syncMu    sync.Mutex
	syncCond  *sync.Cond
	syncing   bool
	syncedSeq uint64
	syncErr   error

	closed bool
}

// Open opens (creating if necessary) a journal directory. Existing segment
// files are scanned so appends continue after the last durable record.
func Open(opts Options) (*Journal, error) {
	if opts.Dir == "" {
		return nil, errors.New("journal: Dir is required")
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

	scan, err := Scan(opts.Dir)
	if err != nil {
		return nil, err
	}
	j.nextSeq = scan.NextSequence
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
	j.nextSeq++
	j.writtenSeq.Store(seq)
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

// WritePart stages a part's payload: begin, data records, end (with length and
// content hash), and commit. It returns a Locator to the data once the part is
// durable per the configured durability mode.
func (j *Journal) WritePart(ctx context.Context, input PartInput, reader io.Reader) (Locator, error) {
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

	if err := j.durable(commitSeq); err != nil {
		return Locator{}, err
	}
	return Locator{
		FileIndex:     fileIndex,
		DataOffset:    dataOffset,
		DataEndOffset: dataEndOffset,
		Length:        length,
		Hash:          hash,
	}, nil
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
	return j.appendLogical(kindActivate, encodeActivate(activatePayload{
		partID:           partID,
		generation:       generation,
		expectedPrevious: expectedPrevious,
	}))
}

// Delete records a logical deletion of partID, optionally asserting which
// generation is being deleted.
func (j *Journal) Delete(ctx context.Context, partID partstore.PartId, expectedGeneration *GenerationID) (uint64, error) {
	return j.appendLogical(kindDelete, encodeDelete(deletePayload{
		partID:             partID,
		expectedGeneration: expectedGeneration,
	}))
}

// Checkpoint records that generation's payload is now durable on a committed
// tape segment, making the journal copy reclaimable during compaction.
func (j *Journal) Checkpoint(ctx context.Context, generation GenerationID, segmentID [16]byte) (uint64, error) {
	return j.appendLogical(kindCheckpoint, encodeCheckpoint(checkpointPayload{
		generation: generation,
		segmentID:  segmentID,
	}))
}

// durable makes all records up to and including seq durable on disk.
func (j *Journal) durable(seq uint64) error {
	if j.durability == DurabilityPerPart {
		j.syncMu.Lock()
		if j.syncedSeq >= seq {
			j.syncMu.Unlock()
			return nil
		}
		j.syncMu.Unlock()
		return j.fsyncAndAdvance()
	}
	return j.waitDurable(seq)
}

// fsyncAndAdvance fsyncs the current file and advances syncedSeq to the
// highest written sequence at the moment of the sync.
func (j *Journal) fsyncAndAdvance() error {
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
				time.Sleep(j.groupPolicy.MaxDelay)
				j.syncMu.Lock()
			}
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

// JournalID returns the identifier shared by this journal's segment files.
func (j *Journal) JournalID() [16]byte { return j.journalID }

// Snapshot rescans the journal directory and returns its durable state. It
// reflects records that are durable on disk (appended and, per the durability
// mode, synced); in-flight writes not yet durable are not included.
func (j *Journal) Snapshot() (*RecoveryResult, error) {
	return Scan(j.dir)
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
