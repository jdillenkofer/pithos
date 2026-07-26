package journal

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type objectLayoutPayload struct {
	objectID partstore.ObjectId
	partIDs  []partstore.PartId
}

func encodeObjectLayout(p objectLayoutPayload) []byte {
	buf := make([]byte, 16+4+16*len(p.partIDs))
	copy(buf[:16], p.objectID[:])
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(p.partIDs)))
	off := 20
	for _, partID := range p.partIDs {
		copy(buf[off:off+16], partID.Bytes())
		off += 16
	}
	return buf
}

func decodeObjectLayout(buf []byte) (objectLayoutPayload, error) {
	if len(buf) < 20 {
		return objectLayoutPayload{}, errors.New("journal: truncated object layout")
	}
	count := int(binary.BigEndian.Uint32(buf[16:20]))
	if count <= 0 || len(buf) != 20+16*count {
		return objectLayoutPayload{}, errors.New("journal: malformed object layout")
	}
	var payload objectLayoutPayload
	copy(payload.objectID[:], buf[:16])
	payload.partIDs = make([]partstore.PartId, 0, count)
	for off := 20; off < len(buf); off += 16 {
		partID, err := partstore.NewPartIdFromBytes(buf[off : off+16])
		if err != nil {
			return objectLayoutPayload{}, err
		}
		payload.partIDs = append(payload.partIDs, *partID)
	}
	return payload, nil
}

// GenerationID uniquely identifies one physical write of a part. A random id
// is trivially crash-safe (no counter to lose) and lets duplicate copies of
// the same logical write deduplicate during recovery.
type GenerationID [16]byte

func NewGenerationID() (GenerationID, error) {
	var g GenerationID
	if _, err := rand.Read(g[:]); err != nil {
		return GenerationID{}, err
	}
	return g, nil
}

func (g GenerationID) String() string { return hex.EncodeToString(g[:]) }

// journalHeaderPayload is the first record of every journal segment file.
type journalHeaderPayload struct {
	journalID       [16]byte
	createdUnixNano int64
	fileIndex       uint64
}

func encodeJournalHeader(p journalHeaderPayload) []byte {
	buf := make([]byte, 16+8+8)
	copy(buf[0:16], p.journalID[:])
	binary.BigEndian.PutUint64(buf[16:24], uint64(p.createdUnixNano))
	binary.BigEndian.PutUint64(buf[24:32], p.fileIndex)
	return buf
}

func decodeJournalHeader(b []byte) (journalHeaderPayload, error) {
	if len(b) != 32 {
		return journalHeaderPayload{}, fmt.Errorf("journal: bad journal-header length %d", len(b))
	}
	var p journalHeaderPayload
	copy(p.journalID[:], b[0:16])
	p.createdUnixNano = int64(binary.BigEndian.Uint64(b[16:24]))
	p.fileIndex = binary.BigEndian.Uint64(b[24:32])
	return p, nil
}

// Hint flags in a part-begin payload.
const (
	hintHasObjectID   = 1 << 0
	hintHasPartNumber = 1 << 1
	hintHasPartCount  = 1 << 2
	hintHasObjectSize = 1 << 3
)

type partBeginPayload struct {
	generation GenerationID
	partID     partstore.PartId
	objectID   *partstore.ObjectId
	partNumber *uint64
	partCount  *uint64
	objectSize *uint64
}

func encodePartBegin(p partBeginPayload) []byte {
	var flags byte
	body := make([]byte, 0, 16+16+1+16+24)
	body = append(body, p.generation[:]...)
	body = append(body, p.partID.Bytes()...)
	// flags byte placeholder appended after computing; build optionals first.
	var optionals []byte
	if p.objectID != nil {
		flags |= hintHasObjectID
		optionals = append(optionals, p.objectID[:]...)
	}
	if p.partNumber != nil {
		flags |= hintHasPartNumber
		optionals = appendUint64(optionals, *p.partNumber)
	}
	if p.partCount != nil {
		flags |= hintHasPartCount
		optionals = appendUint64(optionals, *p.partCount)
	}
	if p.objectSize != nil {
		flags |= hintHasObjectSize
		optionals = appendUint64(optionals, *p.objectSize)
	}
	body = append(body, flags)
	body = append(body, optionals...)
	return body
}

func decodePartBegin(b []byte) (partBeginPayload, error) {
	if len(b) < 33 {
		return partBeginPayload{}, fmt.Errorf("journal: bad part-begin length %d", len(b))
	}
	var p partBeginPayload
	copy(p.generation[:], b[0:16])
	partID, err := partstore.NewPartIdFromBytes(b[16:32])
	if err != nil {
		return partBeginPayload{}, err
	}
	p.partID = *partID
	flags := b[32]
	if flags&^(byte(hintHasObjectID|hintHasPartNumber|hintHasPartCount|hintHasObjectSize)) != 0 {
		return partBeginPayload{}, fmt.Errorf("journal: unknown part-begin flags %#x", flags)
	}
	rest := b[33:]
	read16 := func() ([16]byte, error) {
		var v [16]byte
		if len(rest) < 16 {
			return v, fmt.Errorf("journal: truncated part-begin optional")
		}
		copy(v[:], rest[0:16])
		rest = rest[16:]
		return v, nil
	}
	read64 := func() (uint64, error) {
		if len(rest) < 8 {
			return 0, fmt.Errorf("journal: truncated part-begin optional")
		}
		v := binary.BigEndian.Uint64(rest[0:8])
		rest = rest[8:]
		return v, nil
	}
	if flags&hintHasObjectID != 0 {
		v, err := read16()
		if err != nil {
			return partBeginPayload{}, err
		}
		oid := partstore.ObjectId(v)
		p.objectID = &oid
	}
	if flags&hintHasPartNumber != 0 {
		v, err := read64()
		if err != nil {
			return partBeginPayload{}, err
		}
		p.partNumber = &v
	}
	if flags&hintHasPartCount != 0 {
		v, err := read64()
		if err != nil {
			return partBeginPayload{}, err
		}
		p.partCount = &v
	}
	if flags&hintHasObjectSize != 0 {
		v, err := read64()
		if err != nil {
			return partBeginPayload{}, err
		}
		p.objectSize = &v
	}
	if len(rest) != 0 {
		return partBeginPayload{}, errors.New("journal: trailing part-begin payload")
	}
	return p, nil
}

type partEndPayload struct {
	generation GenerationID
	dataLength uint64
	dataHash   [32]byte
}

func encodePartEnd(p partEndPayload) []byte {
	buf := make([]byte, 16+8+32)
	copy(buf[0:16], p.generation[:])
	binary.BigEndian.PutUint64(buf[16:24], p.dataLength)
	copy(buf[24:56], p.dataHash[:])
	return buf
}

func decodePartEnd(b []byte) (partEndPayload, error) {
	if len(b) != 56 {
		return partEndPayload{}, fmt.Errorf("journal: bad part-end length %d", len(b))
	}
	var p partEndPayload
	copy(p.generation[:], b[0:16])
	p.dataLength = binary.BigEndian.Uint64(b[16:24])
	copy(p.dataHash[:], b[24:56])
	return p, nil
}

func encodeGeneration(g GenerationID) []byte {
	buf := make([]byte, 16)
	copy(buf, g[:])
	return buf
}

func decodeGeneration(b []byte) (GenerationID, error) {
	if len(b) != 16 {
		return GenerationID{}, fmt.Errorf("journal: bad generation length %d", len(b))
	}
	var g GenerationID
	copy(g[:], b)
	return g, nil
}

const (
	activateHasExpectedPrev = 1 << 0
	deleteHasExpectedGen    = 1 << 0
)

type activatePayload struct {
	partID           partstore.PartId
	generation       GenerationID
	expectedPrevious *GenerationID
}

func encodeActivate(p activatePayload) []byte {
	buf := make([]byte, 0, 16+16+1+16)
	buf = append(buf, p.partID.Bytes()...)
	buf = append(buf, p.generation[:]...)
	if p.expectedPrevious != nil {
		buf = append(buf, activateHasExpectedPrev)
		buf = append(buf, p.expectedPrevious[:]...)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

func decodeActivate(b []byte) (activatePayload, error) {
	if len(b) < 33 {
		return activatePayload{}, fmt.Errorf("journal: bad activate length %d", len(b))
	}
	var p activatePayload
	partID, err := partstore.NewPartIdFromBytes(b[0:16])
	if err != nil {
		return activatePayload{}, err
	}
	p.partID = *partID
	copy(p.generation[:], b[16:32])
	if b[32]&^byte(activateHasExpectedPrev) != 0 {
		return activatePayload{}, errors.New("journal: unknown activate flags")
	}
	if b[32]&activateHasExpectedPrev != 0 {
		if len(b) != 49 {
			return activatePayload{}, fmt.Errorf("journal: truncated activate expected-previous")
		}
		var prev GenerationID
		copy(prev[:], b[33:49])
		p.expectedPrevious = &prev
	} else if len(b) != 33 {
		return activatePayload{}, errors.New("journal: trailing activate payload")
	}
	return p, nil
}

type deletePayload struct {
	partID             partstore.PartId
	expectedGeneration *GenerationID
}

func encodeDelete(p deletePayload) []byte {
	buf := make([]byte, 0, 16+1+16)
	buf = append(buf, p.partID.Bytes()...)
	if p.expectedGeneration != nil {
		buf = append(buf, deleteHasExpectedGen)
		buf = append(buf, p.expectedGeneration[:]...)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

func decodeDelete(b []byte) (deletePayload, error) {
	if len(b) < 17 {
		return deletePayload{}, fmt.Errorf("journal: bad delete length %d", len(b))
	}
	var p deletePayload
	partID, err := partstore.NewPartIdFromBytes(b[0:16])
	if err != nil {
		return deletePayload{}, err
	}
	p.partID = *partID
	if b[16]&^byte(deleteHasExpectedGen) != 0 {
		return deletePayload{}, errors.New("journal: unknown delete flags")
	}
	if b[16]&deleteHasExpectedGen != 0 {
		if len(b) != 33 {
			return deletePayload{}, fmt.Errorf("journal: truncated delete expected-generation")
		}
		var g GenerationID
		copy(g[:], b[17:33])
		p.expectedGeneration = &g
	} else if len(b) != 17 {
		return deletePayload{}, errors.New("journal: trailing delete payload")
	}
	return p, nil
}

type checkpointPayload struct {
	generation GenerationID
	segmentID  [16]byte
}

func encodeCheckpoint(p checkpointPayload) []byte {
	buf := make([]byte, 32)
	copy(buf[0:16], p.generation[:])
	copy(buf[16:32], p.segmentID[:])
	return buf
}

func decodeCheckpoint(b []byte) (checkpointPayload, error) {
	if len(b) != 32 {
		return checkpointPayload{}, fmt.Errorf("journal: bad checkpoint length %d", len(b))
	}
	var p checkpointPayload
	copy(p.generation[:], b[0:16])
	copy(p.segmentID[:], b[16:32])
	return p, nil
}

func appendUint64(b []byte, v uint64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	return append(b, tmp[:]...)
}
