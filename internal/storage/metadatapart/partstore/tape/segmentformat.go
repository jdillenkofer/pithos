package tape

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
)

// v2 tape segment format. A segment is one physical tape file (a run of
// records terminated by a single filemark) holding many parts:
//
//	SEGMENT_HEADER
//	(PART_BEGIN PART_DATA* PART_END PART_COMMIT)*
//	(ACTIVATE_PART | DELETE_PART)*
//	INDEX_CHUNK*
//	SEGMENT_FOOTER
//	SEGMENT_COMMIT
//	<filemark>
//
// Every record shares a CRC-protected envelope. A segment is trusted only
// when its header, footer (content hash), and commit (footer hash) all verify
// and the terminating filemark is present; an interrupted tail segment is
// ignored. Filemarks delimit whole segments, never individual parts.
const (
	segmentMagic   = "PTS2"
	segmentVersion = 1

	segEnvelopeSize = 24 // magic[4] ver[1] kind[1] flags[2] payloadLen[4] seq[8] headerCRC[4]
	segPayloadCRC   = 4

	segMaxPayloadSize = 8 << 20
)

const (
	segKindHeader     = 1
	segKindPartBegin  = 2
	segKindPartData   = 3
	segKindPartEnd    = 4
	segKindPartCommit = 5
	segKindActivate   = 6
	segKindDelete     = 7
	segKindIndexChunk = 8
	segKindFooter     = 9
	segKindCommit     = 10
)

var segCRCTable = crc32.MakeTable(crc32.Castagnoli)

func encodeSegmentRecord(kind uint8, sequence uint64, payload []byte) ([]byte, error) {
	if len(payload) > segMaxPayloadSize {
		return nil, fmt.Errorf("tape segment payload too large: %d bytes", len(payload))
	}
	buf := make([]byte, segEnvelopeSize+len(payload)+segPayloadCRC)
	copy(buf[0:4], segmentMagic)
	buf[4] = segmentVersion
	buf[5] = kind
	binary.BigEndian.PutUint16(buf[6:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint64(buf[12:20], sequence)
	binary.BigEndian.PutUint32(buf[20:24], crc32.Checksum(buf[0:20], segCRCTable))
	copy(buf[segEnvelopeSize:], payload)
	binary.BigEndian.PutUint32(buf[segEnvelopeSize+len(payload):], crc32.Checksum(payload, segCRCTable))
	return buf, nil
}

type segRecord struct {
	kind     uint8
	sequence uint64
	payload  []byte
}

// decodeSegmentRecord parses a full tape record. It returns an error for a
// foreign or corrupt record (bad magic/version/CRC), which the scanner treats
// as the end of the trusted region.
func decodeSegmentRecord(buf []byte) (*segRecord, error) {
	if len(buf) < segEnvelopeSize+segPayloadCRC {
		return nil, errors.New("tape segment: record too short")
	}
	if string(buf[0:4]) != segmentMagic {
		return nil, errors.New("tape segment: bad magic")
	}
	if buf[4] != segmentVersion {
		return nil, fmt.Errorf("tape segment: unsupported version %d", buf[4])
	}
	if crc32.Checksum(buf[0:20], segCRCTable) != binary.BigEndian.Uint32(buf[20:24]) {
		return nil, errors.New("tape segment: bad header CRC")
	}
	payloadLen := int(binary.BigEndian.Uint32(buf[8:12]))
	if payloadLen > segMaxPayloadSize || segEnvelopeSize+payloadLen+segPayloadCRC > len(buf) {
		return nil, errors.New("tape segment: bad payload length")
	}
	payload := buf[segEnvelopeSize : segEnvelopeSize+payloadLen]
	wantCRC := binary.BigEndian.Uint32(buf[segEnvelopeSize+payloadLen : segEnvelopeSize+payloadLen+segPayloadCRC])
	if crc32.Checksum(payload, segCRCTable) != wantCRC {
		return nil, errors.New("tape segment: bad payload CRC")
	}
	return &segRecord{
		kind:     buf[5],
		sequence: binary.BigEndian.Uint64(buf[12:20]),
		payload:  payload,
	}, nil
}

// --- payload structs ---

type segmentHeaderPayload struct {
	segmentID       [16]byte
	createdUnixNano int64
	previousSegment [16]byte
	sequenceStart   uint64
}

func encodeSegmentHeader(p segmentHeaderPayload) []byte {
	buf := make([]byte, 16+8+16+8)
	copy(buf[0:16], p.segmentID[:])
	binary.BigEndian.PutUint64(buf[16:24], uint64(p.createdUnixNano))
	copy(buf[24:40], p.previousSegment[:])
	binary.BigEndian.PutUint64(buf[40:48], p.sequenceStart)
	return buf
}

func decodeSegmentHeader(b []byte) (segmentHeaderPayload, error) {
	if len(b) != 48 {
		return segmentHeaderPayload{}, fmt.Errorf("tape segment: bad header length %d", len(b))
	}
	var p segmentHeaderPayload
	copy(p.segmentID[:], b[0:16])
	p.createdUnixNano = int64(binary.BigEndian.Uint64(b[16:24]))
	copy(p.previousSegment[:], b[24:40])
	p.sequenceStart = binary.BigEndian.Uint64(b[40:48])
	return p, nil
}

type segmentFooterPayload struct {
	segmentID        [16]byte
	recordCount      uint64
	partCount        uint64
	logicalOpCount   uint64
	indexStartBlock  uint64
	segmentByteCount uint64
	contentHash      [32]byte
}

func encodeSegmentFooter(p segmentFooterPayload) []byte {
	buf := make([]byte, 16+8*5+32)
	copy(buf[0:16], p.segmentID[:])
	binary.BigEndian.PutUint64(buf[16:24], p.recordCount)
	binary.BigEndian.PutUint64(buf[24:32], p.partCount)
	binary.BigEndian.PutUint64(buf[32:40], p.logicalOpCount)
	binary.BigEndian.PutUint64(buf[40:48], p.indexStartBlock)
	binary.BigEndian.PutUint64(buf[48:56], p.segmentByteCount)
	copy(buf[56:88], p.contentHash[:])
	return buf
}

func decodeSegmentFooter(b []byte) (segmentFooterPayload, error) {
	if len(b) != 88 {
		return segmentFooterPayload{}, fmt.Errorf("tape segment: bad footer length %d", len(b))
	}
	var p segmentFooterPayload
	copy(p.segmentID[:], b[0:16])
	p.recordCount = binary.BigEndian.Uint64(b[16:24])
	p.partCount = binary.BigEndian.Uint64(b[24:32])
	p.logicalOpCount = binary.BigEndian.Uint64(b[32:40])
	p.indexStartBlock = binary.BigEndian.Uint64(b[40:48])
	p.segmentByteCount = binary.BigEndian.Uint64(b[48:56])
	copy(p.contentHash[:], b[56:88])
	return p, nil
}

type segmentCommitPayload struct {
	segmentID  [16]byte
	footerHash [32]byte
}

func encodeSegmentCommit(p segmentCommitPayload) []byte {
	buf := make([]byte, 48)
	copy(buf[0:16], p.segmentID[:])
	copy(buf[16:48], p.footerHash[:])
	return buf
}

func decodeSegmentCommit(b []byte) (segmentCommitPayload, error) {
	if len(b) != 48 {
		return segmentCommitPayload{}, fmt.Errorf("tape segment: bad commit length %d", len(b))
	}
	var p segmentCommitPayload
	copy(p.segmentID[:], b[0:16])
	copy(p.footerHash[:], b[16:48])
	return p, nil
}

// segPartBeginPayload mirrors the journal's part-begin metadata so a migrated
// part keeps its generation and placement hints on tape.
type segPartBeginPayload struct {
	generation journal.GenerationID
	partID     partstore.PartId
	objectID   *partstore.ObjectId
	partNumber *uint64
}

func encodeSegPartBegin(p segPartBeginPayload) []byte {
	var flags byte
	buf := make([]byte, 0, 16+16+1+16+8)
	buf = append(buf, p.generation[:]...)
	buf = append(buf, p.partID.Bytes()...)
	var optionals []byte
	if p.objectID != nil {
		flags |= 1 << 0
		optionals = append(optionals, p.objectID[:]...)
	}
	if p.partNumber != nil {
		flags |= 1 << 1
		var tmp [8]byte
		binary.BigEndian.PutUint64(tmp[:], *p.partNumber)
		optionals = append(optionals, tmp[:]...)
	}
	buf = append(buf, flags)
	buf = append(buf, optionals...)
	return buf
}

func decodeSegPartBegin(b []byte) (segPartBeginPayload, error) {
	if len(b) < 33 {
		return segPartBeginPayload{}, fmt.Errorf("tape segment: bad part-begin length %d", len(b))
	}
	var p segPartBeginPayload
	copy(p.generation[:], b[0:16])
	partID, err := partstore.NewPartIdFromBytes(b[16:32])
	if err != nil {
		return segPartBeginPayload{}, err
	}
	p.partID = *partID
	flags := b[32]
	rest := b[33:]
	if flags&(1<<0) != 0 {
		if len(rest) < 16 {
			return segPartBeginPayload{}, errors.New("tape segment: truncated part-begin object id")
		}
		var oid partstore.ObjectId
		copy(oid[:], rest[0:16])
		p.objectID = &oid
		rest = rest[16:]
	}
	if flags&(1<<1) != 0 {
		if len(rest) < 8 {
			return segPartBeginPayload{}, errors.New("tape segment: truncated part-begin part number")
		}
		v := binary.BigEndian.Uint64(rest[0:8])
		p.partNumber = &v
	}
	return p, nil
}

type segPartEndPayload struct {
	generation journal.GenerationID
	dataLength uint64
	dataHash   [32]byte
}

func encodeSegPartEnd(p segPartEndPayload) []byte {
	buf := make([]byte, 16+8+32)
	copy(buf[0:16], p.generation[:])
	binary.BigEndian.PutUint64(buf[16:24], p.dataLength)
	copy(buf[24:56], p.dataHash[:])
	return buf
}

func decodeSegPartEnd(b []byte) (segPartEndPayload, error) {
	if len(b) != 56 {
		return segPartEndPayload{}, fmt.Errorf("tape segment: bad part-end length %d", len(b))
	}
	var p segPartEndPayload
	copy(p.generation[:], b[0:16])
	p.dataLength = binary.BigEndian.Uint64(b[16:24])
	copy(p.dataHash[:], b[24:56])
	return p, nil
}

// segIndexEntry maps a part+generation to the tape block of its part-begin
// record within the segment.
type segIndexEntry struct {
	generation journal.GenerationID
	partID     partstore.PartId
	startBlock uint64
	dataLength uint64
	dataHash   [32]byte
}

func encodeSegIndexChunk(entries []segIndexEntry) []byte {
	buf := make([]byte, 0, 4+len(entries)*(16+16+8+8+32))
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(entries)))
	buf = append(buf, count[:]...)
	for _, e := range entries {
		buf = append(buf, e.generation[:]...)
		buf = append(buf, e.partID.Bytes()...)
		var tmp [8]byte
		binary.BigEndian.PutUint64(tmp[:], e.startBlock)
		buf = append(buf, tmp[:]...)
		binary.BigEndian.PutUint64(tmp[:], e.dataLength)
		buf = append(buf, tmp[:]...)
		buf = append(buf, e.dataHash[:]...)
	}
	return buf
}

func decodeSegIndexChunk(b []byte) ([]segIndexEntry, error) {
	if len(b) < 4 {
		return nil, errors.New("tape segment: bad index chunk")
	}
	count := int(binary.BigEndian.Uint32(b[0:4]))
	rest := b[4:]
	const entrySize = 16 + 16 + 8 + 8 + 32
	if len(rest) != count*entrySize {
		return nil, fmt.Errorf("tape segment: index chunk length mismatch (%d entries, %d bytes)", count, len(rest))
	}
	entries := make([]segIndexEntry, 0, count)
	for i := range count {
		off := i * entrySize
		var e segIndexEntry
		copy(e.generation[:], rest[off:off+16])
		partID, err := partstore.NewPartIdFromBytes(rest[off+16 : off+32])
		if err != nil {
			return nil, err
		}
		e.partID = *partID
		e.startBlock = binary.BigEndian.Uint64(rest[off+32 : off+40])
		e.dataLength = binary.BigEndian.Uint64(rest[off+40 : off+48])
		copy(e.dataHash[:], rest[off+48:off+80])
		entries = append(entries, e)
	}
	return entries, nil
}

// segActivatePayload / segDeletePayload carry the logical operations captured
// into a segment, mirroring the journal records.
type segActivatePayload struct {
	partID           partstore.PartId
	generation       journal.GenerationID
	expectedPrevious *journal.GenerationID
	sequence         uint64
}

func encodeSegActivate(p segActivatePayload) []byte {
	buf := make([]byte, 0, 16+16+8+1+16)
	buf = append(buf, p.partID.Bytes()...)
	buf = append(buf, p.generation[:]...)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], p.sequence)
	buf = append(buf, tmp[:]...)
	if p.expectedPrevious != nil {
		buf = append(buf, 1)
		buf = append(buf, p.expectedPrevious[:]...)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

func decodeSegActivate(b []byte) (segActivatePayload, error) {
	if len(b) < 41 {
		return segActivatePayload{}, fmt.Errorf("tape segment: bad activate length %d", len(b))
	}
	var p segActivatePayload
	partID, err := partstore.NewPartIdFromBytes(b[0:16])
	if err != nil {
		return segActivatePayload{}, err
	}
	p.partID = *partID
	copy(p.generation[:], b[16:32])
	p.sequence = binary.BigEndian.Uint64(b[32:40])
	if b[40] != 0 {
		if len(b) < 57 {
			return segActivatePayload{}, errors.New("tape segment: truncated activate expected-previous")
		}
		var prev journal.GenerationID
		copy(prev[:], b[41:57])
		p.expectedPrevious = &prev
	}
	return p, nil
}

type segDeletePayload struct {
	partID             partstore.PartId
	expectedGeneration *journal.GenerationID
	sequence           uint64
}

func encodeSegDelete(p segDeletePayload) []byte {
	buf := make([]byte, 0, 16+8+1+16)
	buf = append(buf, p.partID.Bytes()...)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], p.sequence)
	buf = append(buf, tmp[:]...)
	if p.expectedGeneration != nil {
		buf = append(buf, 1)
		buf = append(buf, p.expectedGeneration[:]...)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

func decodeSegDelete(b []byte) (segDeletePayload, error) {
	if len(b) < 25 {
		return segDeletePayload{}, fmt.Errorf("tape segment: bad delete length %d", len(b))
	}
	var p segDeletePayload
	partID, err := partstore.NewPartIdFromBytes(b[0:16])
	if err != nil {
		return segDeletePayload{}, err
	}
	p.partID = *partID
	p.sequence = binary.BigEndian.Uint64(b[16:24])
	if b[24] != 0 {
		if len(b) < 41 {
			return segDeletePayload{}, errors.New("tape segment: truncated delete expected-generation")
		}
		var g journal.GenerationID
		copy(g[:], b[25:41])
		p.expectedGeneration = &g
	}
	return p, nil
}
