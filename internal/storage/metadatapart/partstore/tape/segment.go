package tape

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	tapedev "github.com/jdillenkofer/pithos/internal/tape"
)

// segControlBufferSize bounds reads of control records (header, footer,
// commit, index, part-begin/end, logical ops). Part-data records are never
// read during a scan, so this need only hold the largest control record.
const segControlBufferSize = 256 << 10

// maxIndexEntriesPerChunk bounds an index chunk so it fits segControlBufferSize.
const maxIndexEntriesPerChunk = 2000

// SegmentWriter writes a v2 tape segment at end-of-data. Parts are streamed as
// they arrive; logical operations and the part index are buffered and written
// in the trailer by Finish, which also writes the footer, commit record and
// terminating filemark.
type SegmentWriter struct {
	dev        tapedev.Device
	recordSize int

	segmentID       [16]byte
	previousSegment [16]byte

	seq         uint64
	recordCount uint64
	partCount   uint64
	byteCount   uint64
	content     hash.Hash

	index     []segIndexEntry
	activates []segActivatePayload
	deletes   []segDeletePayload

	finished bool
}

// NewSegmentWriter positions at end-of-data and writes the segment header.
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
	hdr := encodeSegmentHeader(segmentHeaderPayload{
		segmentID:       segmentID,
		createdUnixNano: time.Now().UnixNano(),
		previousSegment: previousSegment,
		sequenceStart:   sequenceStart,
	})
	if err := w.writeContent(ctx, segKindHeader, hdr); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *SegmentWriter) SegmentID() [16]byte { return w.segmentID }

// writeContent writes a record that the footer's content hash covers.
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
	if err := w.writeContent(ctx, segKindPartBegin, encodeSegPartBegin(meta)); err != nil {
		return 0, err
	}
	hasher := sha256.New()
	var length uint64
	buf := make([]byte, w.recordSize)
	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			hasher.Write(buf[:n])
			length += uint64(n)
			if err := w.writeContent(ctx, segKindPartData, buf[:n]); err != nil {
				return 0, err
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return 0, fmt.Errorf("tape segment: reading part content: %w", readErr)
		}
	}
	var dataHash [32]byte
	copy(dataHash[:], hasher.Sum(nil))
	end := segPartEndPayload{generation: meta.generation, dataLength: length, dataHash: dataHash}
	if err := w.writeContent(ctx, segKindPartEnd, encodeSegPartEnd(end)); err != nil {
		return 0, err
	}
	if err := w.writeContent(ctx, segKindPartCommit, meta.generation[:]); err != nil {
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

// AddActivate buffers an activation record for the segment trailer.
func (w *SegmentWriter) AddActivate(p segActivatePayload) { w.activates = append(w.activates, p) }

// AddDelete buffers a deletion record for the segment trailer.
func (w *SegmentWriter) AddDelete(p segDeletePayload) { w.deletes = append(w.deletes, p) }

// Finish writes the trailer (logical ops then index chunks), footer, commit
// record and terminating filemark, then flushes the device. After Finish the
// segment is durable and trusted.
func (w *SegmentWriter) Finish(ctx context.Context) ([16]byte, error) {
	if w.finished {
		return w.segmentID, errors.New("tape segment: writer already finished")
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
	// An empty segment (no ops, no index) still needs a well-defined trailer
	// start; trailerStartBlock then equals the footer block, and the scan
	// reads no trailer records, which is correct.

	var contentHash [32]byte
	copy(contentHash[:], w.content.Sum(nil))
	footer := segmentFooterPayload{
		segmentID:        w.segmentID,
		recordCount:      w.recordCount,
		partCount:        w.partCount,
		logicalOpCount:   opCount,
		indexStartBlock:  trailerStartBlock,
		segmentByteCount: w.byteCount,
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
}

// scanSegments reads every committed segment from the beginning of the tape.
// It verifies each segment's footer and commit and loads its index and logical
// operations without reading part data. Scanning stops at the first
// uncommitted or truncated segment (a crash tail). It also returns tailBlock:
// the block where the trusted region ends, i.e. the clean append point. If
// end-of-data is beyond tailBlock, the bytes in between are an untrusted tail
// the caller should seal.
func scanSegments(ctx context.Context, dev tapedev.Device) (segments []scannedSegment, tailBlock uint64, err error) {
	if err := dev.Rewind(ctx); err != nil {
		return nil, 0, err
	}
	// Recovery deliberately scans forward. The old fast path skipped to the
	// terminating filemark and then sought backwards to the footer and index.
	// On a real drive every backward locate is expensive; a single forward pass
	// verifies the content hash as well as the CRC on every record.
	buf := make([]byte, segMaxPayloadSize+segEnvelopeSize+segPayloadCRC)
	var previousSegment [16]byte

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
			// A record too large to be a control record is not a segment header.
			return segments, segStart, nil
		case err != nil:
			return nil, 0, err
		}
		rec, err := decodeSegmentRecord(buf[:n])
		if err != nil || rec.kind != segKindHeader {
			// Not one of our segment headers: end of the trusted region.
			return segments, segStart, nil
		}
		header, err := decodeSegmentHeader(rec.payload)
		if err != nil {
			return segments, segStart, nil
		}
		if header.previousSegment != previousSegment {
			return segments, segStart, nil
		}

		headerBytes := append([]byte(nil), buf[:n]...)
		seg, ok, err := scanForwardSegment(ctx, dev, buf, segStart, header, headerBytes)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			return segments, segStart, nil
		}
		segments = append(segments, seg)
		previousSegment = seg.header.segmentID
	}
}

// scanForwardSegment consumes the remainder of one physical tape file without
// relocating the head. It validates every record's CRC and the footer's hash
// over all covered records before accepting the segment.
func scanForwardSegment(ctx context.Context, dev tapedev.Device, buf []byte, segStart uint64, header segmentHeaderPayload, headerRecord []byte) (scannedSegment, bool, error) {
	seg := scannedSegment{header: header, firstBlock: segStart}
	content := sha256.New()
	_, _ = content.Write(headerRecord)
	var recordCount, byteCount uint64 = 1, uint64(len(headerRecord))
	var footer *segmentFooterPayload
	var commit *segmentCommitPayload
	var trailerStart uint64

	for {
		pos, err := dev.Tell(ctx)
		if err != nil {
			return scannedSegment{}, false, err
		}
		n, err := dev.ReadRecord(ctx, buf)
		if errors.Is(err, tapedev.ErrFilemark) {
			if footer == nil || commit == nil {
				return scannedSegment{}, false, nil
			}
			if footer.segmentID != header.segmentID || commit.segmentID != header.segmentID || sha256.Sum256(encodeSegmentFooter(*footer)) != commit.footerHash {
				return scannedSegment{}, false, nil
			}
			var gotHash [32]byte
			copy(gotHash[:], content.Sum(nil))
			if footer.contentHash != gotHash || footer.recordCount != recordCount || footer.segmentByteCount != byteCount || footer.partCount != uint64(len(seg.index)) {
				return scannedSegment{}, false, nil
			}
			if trailerStart == 0 {
				trailerStart = pos.Block // empty trailer starts at the footer
			}
			if footer.indexStartBlock != trailerStart {
				return scannedSegment{}, false, nil
			}
			seg.footer = *footer
			return seg, true, nil
		}
		if errors.Is(err, tapedev.ErrEndOfData) || errors.Is(err, io.ErrShortBuffer) {
			return scannedSegment{}, false, nil
		}
		if err != nil {
			return scannedSegment{}, false, err
		}
		rec, err := decodeSegmentRecord(buf[:n])
		if err != nil {
			return scannedSegment{}, false, nil
		}

		switch rec.kind {
		case segKindFooter:
			if footer != nil || commit != nil {
				return scannedSegment{}, false, nil
			}
			decoded, err := decodeSegmentFooter(rec.payload)
			if err != nil {
				return scannedSegment{}, false, nil
			}
			footer = &decoded
			continue
		case segKindCommit:
			if footer == nil || commit != nil {
				return scannedSegment{}, false, nil
			}
			decoded, err := decodeSegmentCommit(rec.payload)
			if err != nil {
				return scannedSegment{}, false, nil
			}
			commit = &decoded
			continue
		}
		if footer != nil { // footer and commit must be terminal records.
			return scannedSegment{}, false, nil
		}
		_, _ = content.Write(buf[:n])
		recordCount++
		byteCount += uint64(n)

		switch rec.kind {
		case segKindActivate:
			if trailerStart == 0 {
				trailerStart = pos.Block
			}
			a, err := decodeSegActivate(rec.payload)
			if err != nil {
				return scannedSegment{}, false, nil
			}
			seg.activates = append(seg.activates, a)
		case segKindDelete:
			if trailerStart == 0 {
				trailerStart = pos.Block
			}
			d, err := decodeSegDelete(rec.payload)
			if err != nil {
				return scannedSegment{}, false, nil
			}
			seg.deletes = append(seg.deletes, d)
		case segKindIndexChunk:
			if trailerStart == 0 {
				trailerStart = pos.Block
			}
			entries, err := decodeSegIndexChunk(rec.payload)
			if err != nil {
				return scannedSegment{}, false, nil
			}
			seg.index = append(seg.index, entries...)
		}
	}
}
