package tape

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
)

// segControlBufferSize bounds reads of control records (header, footer,
// commit, index, part-begin/end, logical ops). Part-data records are never
// read during a scan, so this need only hold the largest control record.
const segControlBufferSize = 256 << 10

// maxIndexEntriesPerChunk bounds an index chunk so it fits segControlBufferSize.
const maxIndexEntriesPerChunk = 2000

// ErrCorruptTape means a complete on-tape record or committed manifest failed
// validation. It is deliberately distinct from an incomplete append tail:
// recovery may seal a torn tail, but must never overwrite media after observed
// corruption.
var ErrCorruptTape = errors.New("tape segment: corrupt media")

// SegmentWriter writes a segment as two physical tape files:
//
//	data records <filemark> compact manifest <filemark>
//
// Recovery can therefore space over the potentially multi-gigabyte data file
// at the drive's high-speed locate rate and read only the compact manifest.
// The manifest is written last and committed independently; a data file without
// a complete manifest is an untrusted crash tail.
type SegmentWriter struct {
	dev        tapedev.Device
	recordSize int

	segmentID       [16]byte
	previousSegment [16]byte
	header          segmentHeaderPayload
	firstBlock      uint64
	endBlock        uint64
	footer          segmentFooterPayload

	seq         uint64
	dataRecords uint64
	dataBytes   uint64
	recordCount uint64
	partCount   uint64
	byteCount   uint64
	content     hash.Hash

	index     []segIndexEntry
	activates []segActivatePayload
	deletes   []segDeletePayload

	finished bool
}

// NewSegmentWriter positions at end-of-data and writes the segment header. If
// writing the header fails, the returned writer identifies the untrusted tail
// that must be sealed before another segment is appended.
func NewSegmentWriter(ctx context.Context, dev tapedev.Device, recordSize int, previousSegment [16]byte, sequenceStart uint64) (*SegmentWriter, error) {
	var segmentID [16]byte
	if _, err := rand.Read(segmentID[:]); err != nil {
		return nil, err
	}
	if recordSize <= 0 {
		recordSize = defaultRecordSize
	}
	w := &SegmentWriter{
		dev:             dev,
		recordSize:      recordSize,
		segmentID:       segmentID,
		previousSegment: previousSegment,
		seq:             sequenceStart,
		content:         sha256.New(),
	}
	if err := dev.SeekToEOD(ctx); err != nil {
		return nil, err
	}
	pos, err := dev.Tell(ctx)
	if err != nil {
		return nil, err
	}
	w.firstBlock = pos.Block
	w.header = segmentHeaderPayload{
		segmentID:       segmentID,
		createdUnixNano: time.Now().UnixNano(),
		previousSegment: previousSegment,
		sequenceStart:   sequenceStart,
	}
	if err := w.writeData(ctx, segKindHeader, encodeSegmentHeader(w.header)); err != nil {
		return w, err
	}
	return w, nil
}

// sealUntrustedTail truncates a failed append and leaves a filemark at its
// starting block. A subsequent writer may then safely append after it.
func sealUntrustedTail(ctx context.Context, dev tapedev.Device, tailBlock uint64) error {
	if err := dev.SeekToEOD(ctx); err != nil {
		return err
	}
	pos, err := dev.Tell(ctx)
	if err != nil {
		return err
	}
	if pos.Block <= tailBlock {
		return nil
	}
	slog.WarnContext(ctx, "Sealing untrusted tape tail", "tailBlock", tailBlock, "eod", pos.Block)
	if err := dev.LocateBlock(ctx, tailBlock); err != nil {
		return err
	}
	if err := dev.WriteFilemarks(ctx, 1); err != nil {
		return err
	}
	return dev.Flush(ctx)
}

func (w *SegmentWriter) SegmentID() [16]byte { return w.segmentID }

// NextSequence is the exact next record sequence after all records written so
// far. It is persisted in the next segment header to keep physical record
// sequences monotonic across variable-sized parts.
func (w *SegmentWriter) NextSequence() uint64 { return w.seq }

// CommittedSegment returns the manifest metadata after Finish succeeds.
func (w *SegmentWriter) CommittedSegment() (scannedSegment, error) {
	if !w.finished {
		return scannedSegment{}, errors.New("tape segment: writer is not finished")
	}
	return scannedSegment{
		header:     w.header,
		footer:     w.footer,
		index:      append([]segIndexEntry(nil), w.index...),
		activates:  append([]segActivatePayload(nil), w.activates...),
		deletes:    append([]segDeletePayload(nil), w.deletes...),
		firstBlock: w.firstBlock,
		endBlock:   w.endBlock,
	}, nil
}

func (w *SegmentWriter) writeData(ctx context.Context, kind uint8, payload []byte) error {
	rec, err := encodeSegmentRecord(kind, w.seq, payload)
	if err != nil {
		return err
	}
	if err := w.dev.WriteRecord(ctx, rec); err != nil {
		return err
	}
	w.dataBytes += uint64(len(rec))
	w.dataRecords++
	w.seq++
	return nil
}

// writeContent writes a compact manifest record covered by the footer hash.
func (w *SegmentWriter) writeContent(ctx context.Context, kind uint8, payload []byte) error {
	rec, err := encodeSegmentRecord(kind, w.seq, payload)
	if err != nil {
		return err
	}
	if err := w.dev.WriteRecord(ctx, rec); err != nil {
		return err
	}
	w.content.Write(rec)
	w.byteCount += uint64(len(rec))
	w.recordCount++
	w.seq++
	return nil
}

// writeTrailer writes the footer or commit, which the content hash excludes
// (the commit hashes the footer, the footer hashes everything before it).
func (w *SegmentWriter) writeTrailer(ctx context.Context, kind uint8, payload []byte) error {
	rec, err := encodeSegmentRecord(kind, w.seq, payload)
	if err != nil {
		return err
	}
	if err := w.dev.WriteRecord(ctx, rec); err != nil {
		return err
	}
	w.seq++
	return nil
}

// WritePart writes one part (begin, data records, end, commit) and records its
// index entry. It returns the tape block of the part-begin record.
func (w *SegmentWriter) WritePart(ctx context.Context, meta segPartBeginPayload, reader io.Reader) (uint64, error) {
	pos, err := w.dev.Tell(ctx)
	if err != nil {
		return 0, err
	}
	startBlock := pos.Block
	if err := w.writeData(ctx, segKindPartBegin, encodeSegPartBegin(meta)); err != nil {
		return 0, err
	}
	length, dataHash, err := w.writePartDataPipelined(ctx, reader)
	if err != nil {
		return 0, err
	}
	end := segPartEndPayload{generation: meta.generation, dataLength: length, dataHash: dataHash}
	if err := w.writeData(ctx, segKindPartEnd, encodeSegPartEnd(end)); err != nil {
		return 0, err
	}
	if err := w.writeData(ctx, segKindPartCommit, meta.generation[:]); err != nil {
		return 0, err
	}
	w.partCount++
	w.index = append(w.index, segIndexEntry{
		generation: meta.generation,
		partID:     meta.partID,
		startBlock: startBlock,
		dataLength: length,
		dataHash:   dataHash,
	})
	return startBlock, nil
}

type segmentDataChunk struct {
	buffer []byte
	n      int
}

type segmentReadResult struct {
	length uint64
	hash   [32]byte
	err    error
}

// writePartDataPipelined overlaps journal reads, record CRC verification, and
// SHA-256 with tape writes. The bounded queue is intentionally small: enough
// to keep a streaming drive fed without allowing migration to consume memory
// proportional to the segment.
func (w *SegmentWriter) writePartDataPipelined(ctx context.Context, reader io.Reader) (uint64, [32]byte, error) {
	const readAheadBytes = 8 << 20
	queueDepth := readAheadBytes / w.recordSize
	queueDepth = max(1, min(queueDepth, 8))

	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	chunks := make(chan segmentDataChunk, queueDepth)
	result := make(chan segmentReadResult, 1)
	done := make(chan struct{})
	var pool sync.Pool
	pool.New = func() any { return make([]byte, w.recordSize) }

	go func() {
		defer close(done)
		defer close(chunks)
		hasher := sha256.New()
		var length uint64
		for {
			buffer := pool.Get().([]byte)
			n, readErr := io.ReadFull(reader, buffer)
			if n > 0 {
				_, _ = hasher.Write(buffer[:n])
				length += uint64(n)
				select {
				case chunks <- segmentDataChunk{buffer: buffer, n: n}:
				case <-pipelineCtx.Done():
					pool.Put(buffer)
					result <- segmentReadResult{err: pipelineCtx.Err()}
					return
				}
			} else {
				pool.Put(buffer)
			}
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				var sum [32]byte
				copy(sum[:], hasher.Sum(nil))
				result <- segmentReadResult{length: length, hash: sum}
				return
			}
			if readErr != nil {
				result <- segmentReadResult{err: fmt.Errorf("tape segment: reading part content: %w", readErr)}
				return
			}
		}
	}()

	for chunk := range chunks {
		if err := w.writeData(ctx, segKindPartData, chunk.buffer[:chunk.n]); err != nil {
			pool.Put(chunk.buffer)
			cancel()
			<-done
			return 0, [32]byte{}, err
		}
		pool.Put(chunk.buffer)
	}
	readResult := <-result
	if readResult.err != nil {
		return 0, [32]byte{}, readResult.err
	}
	return readResult.length, readResult.hash, nil
}

// AddActivate buffers an activation record for the segment trailer.
func (w *SegmentWriter) AddActivate(p segActivatePayload) { w.activates = append(w.activates, p) }

// AddDelete buffers a deletion record for the segment trailer.
func (w *SegmentWriter) AddDelete(p segDeletePayload) { w.deletes = append(w.deletes, p) }

// Finish closes the data file, writes the compact manifest (logical ops and
// index), then seals that manifest with its footer, commit, and filemark.
// Only the manifest records are covered by footer.contentHash; payload
// integrity is represented by each index entry's SHA-256 and verified on
// recall.
func (w *SegmentWriter) Finish(ctx context.Context) ([16]byte, error) {
	if w.finished {
		return w.segmentID, errors.New("tape segment: writer already finished")
	}
	if err := w.dev.WriteFilemarks(ctx, 1); err != nil {
		return w.segmentID, err
	}
	pos, err := w.dev.Tell(ctx)
	if err != nil {
		return w.segmentID, err
	}
	trailerStartBlock := pos.Block

	var opCount uint64
	for _, a := range w.activates {
		if err := w.writeContent(ctx, segKindActivate, encodeSegActivate(a)); err != nil {
			return w.segmentID, err
		}
		opCount++
	}
	for _, d := range w.deletes {
		if err := w.writeContent(ctx, segKindDelete, encodeSegDelete(d)); err != nil {
			return w.segmentID, err
		}
		opCount++
	}
	for start := 0; start < len(w.index); start += maxIndexEntriesPerChunk {
		end := min(start+maxIndexEntriesPerChunk, len(w.index))
		if err := w.writeContent(ctx, segKindIndexChunk, encodeSegIndexChunk(w.index[start:end])); err != nil {
			return w.segmentID, err
		}
	}
	var contentHash [32]byte
	copy(contentHash[:], w.content.Sum(nil))
	footer := segmentFooterPayload{
		segmentID:        w.segmentID,
		recordCount:      w.recordCount,
		partCount:        w.partCount,
		logicalOpCount:   opCount,
		indexStartBlock:  trailerStartBlock,
		segmentByteCount: w.byteCount,
		nextSequence:     w.seq + 2, // footer and commit consume two sequences
		contentHash:      contentHash,
	}
	footerBytes := encodeSegmentFooter(footer)
	if err := w.writeTrailer(ctx, segKindFooter, footerBytes); err != nil {
		return w.segmentID, err
	}
	footerHash := sha256.Sum256(footerBytes)
	commit := segmentCommitPayload{segmentID: w.segmentID, footerHash: footerHash}
	if err := w.writeTrailer(ctx, segKindCommit, encodeSegmentCommit(commit)); err != nil {
		return w.segmentID, err
	}
	if err := w.dev.WriteFilemarks(ctx, 1); err != nil {
		return w.segmentID, err
	}
	if err := w.dev.Flush(ctx); err != nil {
		return w.segmentID, err
	}
	pos, err = w.dev.Tell(ctx)
	if err != nil {
		return w.segmentID, err
	}
	w.footer = footer
	w.endBlock = pos.Block
	w.finished = true
	return w.segmentID, nil
}

// scannedSegment is the verified metadata of a committed tape segment.
type scannedSegment struct {
	header    segmentHeaderPayload
	footer    segmentFooterPayload
	index     []segIndexEntry
	activates []segActivatePayload
	deletes   []segDeletePayload
	// firstBlock is the tape block of the segment header record.
	firstBlock uint64
	// endBlock is the clean append position immediately after the manifest
	// filemark.
	endBlock uint64
}

// scanSegments reads committed segment manifests from the beginning of the
// tape. It never reads part data: after reading a segment header it spaces to
// the data-file filemark, then verifies the compact manifest. Scanning stops at
// the first
// uncommitted or truncated segment (a crash tail). It also returns tailBlock:
// the block where the trusted region ends, i.e. the clean append point. If
// end-of-data is beyond tailBlock, the bytes in between are an untrusted tail
// the caller should seal.
func scanSegments(ctx context.Context, dev tapedev.Device) (segments []scannedSegment, tailBlock uint64, err error) {
	if err := dev.Rewind(ctx); err != nil {
		return nil, 0, err
	}
	return scanSegmentsFrom(ctx, dev, 0, [16]byte{})
}

// scanSegmentsFrom incrementally scans at startBlock, validating that the
// first discovered segment chains from previousSegment.
func scanSegmentsFrom(ctx context.Context, dev tapedev.Device, startBlock uint64, previousSegment [16]byte) (segments []scannedSegment, tailBlock uint64, err error) {
	if err := dev.LocateBlock(ctx, startBlock); err != nil {
		if errors.Is(err, tapedev.ErrEndOfData) {
			return nil, 0, fmt.Errorf("%w: catalog tail block %d is beyond end of data", ErrCorruptTape, startBlock)
		}
		return nil, 0, err
	}
	buf := make([]byte, segControlBufferSize)

	for {
		pos, err := dev.Tell(ctx)
		if err != nil {
			return nil, 0, err
		}
		segStart := pos.Block

		n, err := dev.ReadRecord(ctx, buf)
		switch {
		case errors.Is(err, tapedev.ErrEndOfData):
			return segments, segStart, nil
		case errors.Is(err, tapedev.ErrFilemark):
			// Stray filemark (e.g. a sealed truncated tail); skip it.
			continue
		case errors.Is(err, io.ErrShortBuffer):
			return nil, 0, fmt.Errorf("%w: oversized segment header at block %d", ErrCorruptTape, segStart)
		case err != nil:
			return nil, 0, err
		}
		rec, err := decodeSegmentRecord(buf[:n])
		if err != nil {
			return nil, 0, fmt.Errorf("%w: invalid segment header at block %d: %v", ErrCorruptTape, segStart, err)
		}
		if rec.kind != segKindHeader {
			return nil, 0, fmt.Errorf("%w: unexpected record kind %d at segment boundary block %d", ErrCorruptTape, rec.kind, segStart)
		}
		header, err := decodeSegmentHeader(rec.payload)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: invalid segment header payload at block %d: %v", ErrCorruptTape, segStart, err)
		}
		if rec.sequence != header.sequenceStart {
			return nil, 0, fmt.Errorf("%w: segment sequence starts at %d but header record is %d", ErrCorruptTape, header.sequenceStart, rec.sequence)
		}
		if header.previousSegment != previousSegment {
			return nil, 0, fmt.Errorf("%w: broken segment chain at block %d", ErrCorruptTape, segStart)
		}

		// Skip the rest of the large data file without streaming its payload.
		if err := dev.SpaceFilemarks(ctx, 1); err != nil {
			if errors.Is(err, tapedev.ErrEndOfData) {
				return segments, segStart, nil
			}
			return nil, 0, err
		}
		manifestPos, err := dev.Tell(ctx)
		if err != nil {
			return nil, 0, err
		}
		seg, ok, err := scanSegmentManifest(ctx, dev, buf, segStart, manifestPos.Block, header)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			return segments, segStart, nil
		}
		end, err := dev.Tell(ctx)
		if err != nil {
			return nil, 0, err
		}
		seg.endBlock = end.Block
		segments = append(segments, seg)
		previousSegment = seg.header.segmentID
	}
}

// scanSegmentManifest consumes and verifies the small manifest tape file.
func scanSegmentManifest(ctx context.Context, dev tapedev.Device, buf []byte, segStart, manifestStart uint64, header segmentHeaderPayload) (scannedSegment, bool, error) {
	seg := scannedSegment{header: header, firstBlock: segStart}
	content := sha256.New()
	var recordCount, byteCount uint64
	var footer *segmentFooterPayload
	var commit *segmentCommitPayload
	var previousSequence uint64
	haveSequence := false
	indexPhase := false
	generations := make(map[journal.GenerationID]struct{})

	for {
		n, err := dev.ReadRecord(ctx, buf)
		if errors.Is(err, tapedev.ErrFilemark) {
			if footer == nil || commit == nil {
				return scannedSegment{}, false, nil
			}
			if footer.segmentID != header.segmentID || commit.segmentID != header.segmentID || sha256.Sum256(encodeSegmentFooter(*footer)) != commit.footerHash {
				return scannedSegment{}, false, fmt.Errorf("%w: segment %x footer/commit mismatch", ErrCorruptTape, header.segmentID)
			}
			var gotHash [32]byte
			copy(gotHash[:], content.Sum(nil))
			if footer.contentHash != gotHash || footer.recordCount != recordCount || footer.segmentByteCount != byteCount ||
				footer.partCount != uint64(len(seg.index)) ||
				footer.logicalOpCount != uint64(len(seg.activates)+len(seg.deletes)) {
				return scannedSegment{}, false, fmt.Errorf("%w: segment %x manifest totals mismatch", ErrCorruptTape, header.segmentID)
			}
			if footer.indexStartBlock != manifestStart {
				return scannedSegment{}, false, fmt.Errorf("%w: segment %x manifest block mismatch", ErrCorruptTape, header.segmentID)
			}
			if footer.nextSequence != previousSequence+1 {
				return scannedSegment{}, false, fmt.Errorf("%w: segment %x next sequence mismatch", ErrCorruptTape, header.segmentID)
			}
			seg.footer = *footer
			return seg, true, nil
		}
		if errors.Is(err, tapedev.ErrEndOfData) {
			return scannedSegment{}, false, nil
		}
		if errors.Is(err, io.ErrShortBuffer) {
			return scannedSegment{}, false, fmt.Errorf("%w: oversized manifest record for segment %x", ErrCorruptTape, header.segmentID)
		}
		if err != nil {
			return scannedSegment{}, false, err
		}
		rec, err := decodeSegmentRecord(buf[:n])
		if err != nil {
			return scannedSegment{}, false, fmt.Errorf("%w: invalid manifest record for segment %x: %v", ErrCorruptTape, header.segmentID, err)
		}
		if haveSequence && rec.sequence != previousSequence+1 {
			return scannedSegment{}, false, fmt.Errorf("%w: non-contiguous manifest sequence %d after %d", ErrCorruptTape, rec.sequence, previousSequence)
		}
		previousSequence = rec.sequence
		haveSequence = true

		switch rec.kind {
		case segKindFooter:
			if footer != nil || commit != nil {
				return scannedSegment{}, false, fmt.Errorf("%w: duplicate/out-of-order footer", ErrCorruptTape)
			}
			decoded, err := decodeSegmentFooter(rec.payload)
			if err != nil {
				return scannedSegment{}, false, fmt.Errorf("%w: invalid footer: %v", ErrCorruptTape, err)
			}
			footer = &decoded
			continue
		case segKindCommit:
			if footer == nil || commit != nil {
				return scannedSegment{}, false, fmt.Errorf("%w: duplicate/out-of-order commit", ErrCorruptTape)
			}
			decoded, err := decodeSegmentCommit(rec.payload)
			if err != nil {
				return scannedSegment{}, false, fmt.Errorf("%w: invalid commit: %v", ErrCorruptTape, err)
			}
			commit = &decoded
			continue
		}
		if footer != nil { // footer and commit must be terminal records.
			return scannedSegment{}, false, fmt.Errorf("%w: content after footer", ErrCorruptTape)
		}
		_, _ = content.Write(buf[:n])
		recordCount++
		byteCount += uint64(n)

		switch rec.kind {
		case segKindActivate:
			if indexPhase {
				return scannedSegment{}, false, fmt.Errorf("%w: activation after index", ErrCorruptTape)
			}
			a, err := decodeSegActivate(rec.payload)
			if err != nil {
				return scannedSegment{}, false, fmt.Errorf("%w: invalid activation: %v", ErrCorruptTape, err)
			}
			seg.activates = append(seg.activates, a)
		case segKindDelete:
			if indexPhase {
				return scannedSegment{}, false, fmt.Errorf("%w: deletion after index", ErrCorruptTape)
			}
			d, err := decodeSegDelete(rec.payload)
			if err != nil {
				return scannedSegment{}, false, fmt.Errorf("%w: invalid deletion: %v", ErrCorruptTape, err)
			}
			seg.deletes = append(seg.deletes, d)
		case segKindIndexChunk:
			indexPhase = true
			entries, err := decodeSegIndexChunk(rec.payload)
			if err != nil {
				return scannedSegment{}, false, fmt.Errorf("%w: invalid index: %v", ErrCorruptTape, err)
			}
			for _, entry := range entries {
				if entry.startBlock <= segStart || entry.startBlock >= manifestStart {
					return scannedSegment{}, false, fmt.Errorf("%w: indexed block %d is outside segment data", ErrCorruptTape, entry.startBlock)
				}
				if _, duplicate := generations[entry.generation]; duplicate {
					return scannedSegment{}, false, fmt.Errorf("%w: duplicate generation %s in segment index", ErrCorruptTape, entry.generation.String())
				}
				generations[entry.generation] = struct{}{}
			}
			seg.index = append(seg.index, entries...)
		default:
			return scannedSegment{}, false, fmt.Errorf("%w: unexpected manifest record kind %d", ErrCorruptTape, rec.kind)
		}
	}
}
