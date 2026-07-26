package journal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// RecoveredPart is a part whose payload is durably committed in the journal.
type RecoveredPart struct {
	Generation        GenerationID
	PartID            partstore.PartId
	Location          Locator
	ObjectID          *partstore.ObjectId
	PartNumber        *uint64
	PartCount         *uint64
	Length            uint64
	Hash              [32]byte
	Checkpointed      bool
	CheckpointSegment [16]byte
}

// LogicalOp is an activation or deletion, ordered by Sequence.
type LogicalOp struct {
	Sequence           uint64
	Kind               uint8
	PartID             partstore.PartId
	Generation         GenerationID
	ExpectedPrevious   *GenerationID
	ExpectedGeneration *GenerationID
}

// IsActivate reports whether the op is an activation.
func (o LogicalOp) IsActivate() bool { return o.Kind == kindActivate }

// IsDelete reports whether the op is a deletion.
func (o LogicalOp) IsDelete() bool { return o.Kind == kindDelete }

// RecoveryResult is the state reconstructed from a journal directory.
type RecoveryResult struct {
	JournalID    [16]byte
	MaxFileIndex uint64
	NextSequence uint64
	// Parts holds every committed part payload, keyed by generation.
	Parts map[GenerationID]*RecoveredPart
	// Ops is the ordered logical operation stream.
	Ops []LogicalOp
	// Live maps each live part to its active generation.
	Live map[partstore.PartId]GenerationID
	// partsByID accelerates placement finalization; it is reconstructed from
	// Parts and is not part of the public recovery contract.
	partsByID map[partstore.PartId][]GenerationID
	layouts   map[partstore.PartId]recoveredPlacement
}

type recoveredPlacement struct {
	objectID   partstore.ObjectId
	partNumber uint64
	partCount  uint64
}

func cloneRecoveryResult(src *RecoveryResult) *RecoveryResult {
	dst := &RecoveryResult{
		JournalID:    src.JournalID,
		MaxFileIndex: src.MaxFileIndex,
		NextSequence: src.NextSequence,
		Parts:        make(map[GenerationID]*RecoveredPart, len(src.Parts)),
		Ops:          make([]LogicalOp, len(src.Ops)),
		Live:         make(map[partstore.PartId]GenerationID),
		partsByID:    make(map[partstore.PartId][]GenerationID),
		layouts:      make(map[partstore.PartId]recoveredPlacement),
	}
	for generation, part := range src.Parts {
		copy := *part
		if part.ObjectID != nil {
			value := *part.ObjectID
			copy.ObjectID = &value
		}
		if part.PartNumber != nil {
			value := *part.PartNumber
			copy.PartNumber = &value
		}
		if part.PartCount != nil {
			value := *part.PartCount
			copy.PartCount = &value
		}
		dst.Parts[generation] = &copy
		dst.partsByID[copy.PartID] = append(dst.partsByID[copy.PartID], generation)
	}
	for i, op := range src.Ops {
		dst.Ops[i] = op
		if op.ExpectedPrevious != nil {
			value := *op.ExpectedPrevious
			dst.Ops[i].ExpectedPrevious = &value
		}
		if op.ExpectedGeneration != nil {
			value := *op.ExpectedGeneration
			dst.Ops[i].ExpectedGeneration = &value
		}
	}
	for partID, placement := range src.layouts {
		dst.layouts[partID] = placement
	}
	return dst
}

func rebuildLive(res *RecoveryResult) {
	res.Live = make(map[partstore.PartId]GenerationID)
	sort.Slice(res.Ops, func(a, b int) bool { return res.Ops[a].Sequence < res.Ops[b].Sequence })
	for _, op := range res.Ops {
		switch op.Kind {
		case kindActivate:
			if op.ExpectedPrevious != nil {
				current, live := res.Live[op.PartID]
				if !live || current != *op.ExpectedPrevious {
					continue
				}
			}
			res.Live[op.PartID] = op.Generation
		case kindDelete:
			if op.ExpectedGeneration != nil {
				current, live := res.Live[op.PartID]
				if !live || current != *op.ExpectedGeneration {
					continue
				}
			}
			delete(res.Live, op.PartID)
		}
	}
}

// Scan reconstructs journal state from a directory. It ignores torn tails
// (records truncated or corrupted by a crash) and everything after them
// within a file.
func Scan(dir string) (*RecoveryResult, error) {
	indices, err := sortedFileIndices(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &RecoveryResult{NextSequence: 1, Parts: map[GenerationID]*RecoveredPart{}, Live: map[partstore.PartId]GenerationID{}, partsByID: map[partstore.PartId][]GenerationID{}, layouts: map[partstore.PartId]recoveredPlacement{}}, nil
		}
		return nil, err
	}

	res := &RecoveryResult{
		NextSequence: 1,
		Parts:        map[GenerationID]*RecoveredPart{},
		Live:         map[partstore.PartId]GenerationID{},
		partsByID:    map[partstore.PartId][]GenerationID{},
		layouts:      map[partstore.PartId]recoveredPlacement{},
	}
	pending := map[GenerationID]*RecoveredPart{}
	var maxSeq uint64
	haveJournalID := false

	for _, idx := range indices {
		if idx > res.MaxFileIndex {
			res.MaxFileIndex = idx
		}
		path := filepath.Join(dir, fileName(idx))
		if err := scanFile(path, idx, res, pending, &maxSeq, &haveJournalID); err != nil {
			return nil, err
		}
	}

	res.NextSequence = maxSeq + 1

	rebuildLive(res)

	return res, nil
}

func scanFile(path string, fileIndex uint64, res *RecoveryResult, pending map[GenerationID]*RecoveredPart, maxSeq *uint64, haveJournalID *bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)

	var offset int64
	recordIndex := 0
	hdrBuf := make([]byte, envelopeHeaderSize)
	for {
		if _, err := io.ReadFull(r, hdrBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// A truncated final record is a normal crash artifact. Journal
				// files are immutable after restart, so a later file may
				// legitimately follow such a tail.
				return nil
			}
			return err
		}
		h, err := decodeHeader(hdrBuf)
		if err != nil {
			// A complete envelope header with an invalid checksum/magic is
			// corruption, not a torn syscall. Fail closed rather than silently
			// discarding all logical operations that follow it.
			return fmt.Errorf("journal: corrupt record header in %s at offset %d: %w", path, offset, err)
		}
		payload := make([]byte, int(h.payloadLen)+payloadCRCSize)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		full := make([]byte, h.payloadSize)
		copy(full, hdrBuf)
		copy(full[envelopeHeaderSize:], payload)
		body, err := verifyPayload(full, h)
		if err != nil {
			return fmt.Errorf("journal: corrupt record payload in %s at offset %d: %w", path, offset, err)
		}
		if recordIndex == 0 && h.kind != kindJournalHeader {
			return fmt.Errorf("journal: file %d does not start with a journal header", fileIndex)
		}
		if recordIndex > 0 && h.kind == kindJournalHeader {
			return fmt.Errorf("journal: unexpected journal header in file %d at offset %d", fileIndex, offset)
		}
		if *maxSeq > 0 && h.sequence <= *maxSeq {
			return fmt.Errorf("journal: non-increasing sequence %d after %d", h.sequence, *maxSeq)
		}

		recordOffset := offset
		offset += int64(h.payloadSize)
		if h.sequence > *maxSeq {
			*maxSeq = h.sequence
		}

		if err := applyRecord(h, body, fileIndex, recordOffset, offset, res, pending, haveJournalID); err != nil {
			return err
		}
		recordIndex++
	}
}

func applyRecord(h *recordHeader, body []byte, fileIndex uint64, recordOffset, nextOffset int64, res *RecoveryResult, pending map[GenerationID]*RecoveredPart, haveJournalID *bool) error {
	switch h.kind {
	case kindJournalHeader:
		hdr, err := decodeJournalHeader(body)
		if err != nil {
			return err
		}
		if hdr.fileIndex != fileIndex {
			return fmt.Errorf("journal: header file index %d does not match filename index %d", hdr.fileIndex, fileIndex)
		}
		if !*haveJournalID {
			res.JournalID = hdr.journalID
			*haveJournalID = true
		} else if res.JournalID != hdr.journalID {
			return fmt.Errorf("journal: file %d belongs to a different journal", fileIndex)
		}
	case kindPartBegin:
		p, err := decodePartBegin(body)
		if err != nil {
			return err
		}
		pending[p.generation] = &RecoveredPart{
			Generation: p.generation,
			PartID:     p.partID,
			ObjectID:   p.objectID,
			PartNumber: p.partNumber,
			PartCount:  p.partCount,
			Location:   Locator{FileIndex: fileIndex, DataOffset: nextOffset, DataEndOffset: nextOffset},
		}
	case kindPartData:
		// Part-data records belong to the most recently begun part in this
		// file. Extend its data range; the surrounding begin/end scope it.
		for _, part := range pending {
			if part.Location.FileIndex == fileIndex && part.Location.DataEndOffset == recordOffset {
				part.Location.DataEndOffset = nextOffset
				break
			}
		}
	case kindPartEnd:
		p, err := decodePartEnd(body)
		if err != nil {
			return err
		}
		if part, ok := pending[p.generation]; ok {
			part.Length = p.dataLength
			part.Hash = p.dataHash
			part.Location.Length = p.dataLength
			part.Location.Hash = p.dataHash
		}
	case kindPartCommit:
		gen, err := decodeGeneration(body)
		if err != nil {
			return err
		}
		if part, ok := pending[gen]; ok {
			applyRecoveredPlacement(res, part)
			res.Parts[gen] = part
			res.partsByID[part.PartID] = append(res.partsByID[part.PartID], gen)
			delete(pending, gen)
		}
	case kindActivate:
		p, err := decodeActivate(body)
		if err != nil {
			return err
		}
		res.Ops = append(res.Ops, LogicalOp{
			Sequence:         h.sequence,
			Kind:             kindActivate,
			PartID:           p.partID,
			Generation:       p.generation,
			ExpectedPrevious: p.expectedPrevious,
		})
	case kindDelete:
		p, err := decodeDelete(body)
		if err != nil {
			return err
		}
		res.Ops = append(res.Ops, LogicalOp{
			Sequence:           h.sequence,
			Kind:               kindDelete,
			PartID:             p.partID,
			ExpectedGeneration: p.expectedGeneration,
		})
	case kindCheckpoint:
		p, err := decodeCheckpoint(body)
		if err != nil {
			return err
		}
		if part, ok := res.Parts[p.generation]; ok {
			part.Checkpointed = true
			part.CheckpointSegment = p.segmentID
		}
	case kindObjectLayout:
		layout, err := decodeObjectLayout(body)
		if err != nil {
			return err
		}
		applyObjectLayout(res, layout)
	}
	return nil
}

func applyRecoveredPlacement(res *RecoveryResult, part *RecoveredPart) {
	placement, ok := res.layouts[part.PartID]
	if !ok {
		return
	}
	objectID := placement.objectID
	partNumber := placement.partNumber
	partCount := placement.partCount
	part.ObjectID = &objectID
	part.PartNumber = &partNumber
	part.PartCount = &partCount
}
