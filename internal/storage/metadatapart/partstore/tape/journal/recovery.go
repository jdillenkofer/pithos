package journal

import (
	"bufio"
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
}

// Scan reconstructs journal state from a directory. It ignores torn tails
// (records truncated or corrupted by a crash) and everything after them
// within a file.
func Scan(dir string) (*RecoveryResult, error) {
	indices, err := sortedFileIndices(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &RecoveryResult{NextSequence: 1, Parts: map[GenerationID]*RecoveredPart{}, Live: map[partstore.PartId]GenerationID{}}, nil
		}
		return nil, err
	}

	res := &RecoveryResult{
		NextSequence: 1,
		Parts:        map[GenerationID]*RecoveredPart{},
		Live:         map[partstore.PartId]GenerationID{},
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

	// Replay logical operations in global sequence order to derive the live
	// set. Physical existence of a payload does not imply visibility.
	sort.Slice(res.Ops, func(a, b int) bool { return res.Ops[a].Sequence < res.Ops[b].Sequence })
	for _, op := range res.Ops {
		switch op.Kind {
		case kindActivate:
			part, ok := res.Parts[op.Generation]
			if !ok {
				// Activation references a payload that is not durably committed;
				// retain the previous live generation.
				continue
			}
			if op.ExpectedPrevious != nil {
				current, live := res.Live[op.PartID]
				if !live || current != *op.ExpectedPrevious {
					continue
				}
			}
			_ = part
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
	hdrBuf := make([]byte, envelopeHeaderSize)
	for {
		if _, err := io.ReadFull(r, hdrBuf); err != nil {
			// A truncated header at the tail is a normal crash artifact.
			return nil
		}
		h, err := decodeHeader(hdrBuf)
		if err != nil {
			// A torn or corrupt record ends the readable region of the file.
			return nil
		}
		payload := make([]byte, int(h.payloadLen)+payloadCRCSize)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil
		}
		full := make([]byte, h.payloadSize)
		copy(full, hdrBuf)
		copy(full[envelopeHeaderSize:], payload)
		body, err := verifyPayload(full, h)
		if err != nil {
			return nil
		}

		recordOffset := offset
		offset += int64(h.payloadSize)
		if h.sequence > *maxSeq {
			*maxSeq = h.sequence
		}

		if err := applyRecord(h, body, fileIndex, recordOffset, offset, res, pending, haveJournalID); err != nil {
			return err
		}
	}
}

func applyRecord(h *recordHeader, body []byte, fileIndex uint64, recordOffset, nextOffset int64, res *RecoveryResult, pending map[GenerationID]*RecoveredPart, haveJournalID *bool) error {
	switch h.kind {
	case kindJournalHeader:
		hdr, err := decodeJournalHeader(body)
		if err != nil {
			return err
		}
		if !*haveJournalID {
			res.JournalID = hdr.journalID
			*haveJournalID = true
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
			res.Parts[gen] = part
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
	}
	return nil
}
