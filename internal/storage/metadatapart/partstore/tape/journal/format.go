package journal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// On-disk journal layout. A journal directory holds one or more append-only
// segment files (journal-<n>.pj). Every record is framed by a fixed envelope
// followed by a variable payload and a payload CRC:
//
//	envelope: magic[4] version[1] kind[1] flags[2] payloadLen[4]
//	          sequence[8] headerCRC[4]
//	payload:  payloadLen bytes
//	trailer:  payloadCRC[4]
//
// headerCRC covers the 20 envelope bytes before it; payloadCRC covers the
// payload. A record whose CRCs do not verify, or that is truncated, ends the
// readable region of a file (torn tail from a crash) and everything after it
// is ignored.
const (
	envelopeMagic   = "PJRN"
	envelopeVersion = 1

	envelopeHeaderSize = 24 // magic+version+kind+flags+payloadLen+sequence+headerCRC
	payloadCRCSize     = 4

	// maxPayloadSize bounds a single record's payload to guard recovery
	// against a corrupt length field allocating unbounded memory.
	maxPayloadSize = 64 << 20
)

// Record kinds.
const (
	kindJournalHeader = 1
	kindPartBegin     = 2
	kindPartData      = 3
	kindPartEnd       = 4
	kindPartCommit    = 5
	kindActivate      = 6
	kindDelete        = 7
	kindCheckpoint    = 8
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// encodeRecord frames one record. sequence is the record's global monotonic
// position; payload is the already-encoded record body.
func encodeRecord(kind uint8, flags uint16, sequence uint64, payload []byte) ([]byte, error) {
	if len(payload) > maxPayloadSize {
		return nil, fmt.Errorf("journal payload too large: %d bytes", len(payload))
	}
	buf := make([]byte, envelopeHeaderSize+len(payload)+payloadCRCSize)
	copy(buf[0:4], envelopeMagic)
	buf[4] = envelopeVersion
	buf[5] = kind
	binary.BigEndian.PutUint16(buf[6:8], flags)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint64(buf[12:20], sequence)
	binary.BigEndian.PutUint32(buf[20:24], crc32.Checksum(buf[0:20], crcTable))
	copy(buf[envelopeHeaderSize:], payload)
	binary.BigEndian.PutUint32(buf[envelopeHeaderSize+len(payload):], crc32.Checksum(payload, crcTable))
	return buf, nil
}

// recordHeader is the decoded envelope of a record.
type recordHeader struct {
	kind        uint8
	flags       uint16
	payloadLen  uint32
	sequence    uint64
	payloadSize int // total on-disk size: envelope + payload + crc
}

var errShortHeader = errors.New("journal: truncated record header")

// decodeHeader parses an envelope from the front of buf. It returns
// errShortHeader when buf is smaller than an envelope, and a descriptive
// error when the magic/version/headerCRC do not verify (a torn or foreign
// record).
func decodeHeader(buf []byte) (*recordHeader, error) {
	if len(buf) < envelopeHeaderSize {
		return nil, errShortHeader
	}
	if string(buf[0:4]) != envelopeMagic {
		return nil, errors.New("journal: bad record magic")
	}
	if buf[4] != envelopeVersion {
		return nil, fmt.Errorf("journal: unsupported record version %d", buf[4])
	}
	wantCRC := binary.BigEndian.Uint32(buf[20:24])
	if crc32.Checksum(buf[0:20], crcTable) != wantCRC {
		return nil, errors.New("journal: bad header CRC")
	}
	payloadLen := binary.BigEndian.Uint32(buf[8:12])
	if payloadLen > maxPayloadSize {
		return nil, fmt.Errorf("journal: record payload length %d exceeds limit", payloadLen)
	}
	return &recordHeader{
		kind:        buf[5],
		flags:       binary.BigEndian.Uint16(buf[6:8]),
		payloadLen:  payloadLen,
		sequence:    binary.BigEndian.Uint64(buf[12:20]),
		payloadSize: envelopeHeaderSize + int(payloadLen) + payloadCRCSize,
	}, nil
}

// verifyPayload checks the payload CRC that trails the payload in record.
// record must be the full on-disk record (envelope + payload + crc).
func verifyPayload(record []byte, h *recordHeader) ([]byte, error) {
	payload := record[envelopeHeaderSize : envelopeHeaderSize+int(h.payloadLen)]
	gotCRC := binary.BigEndian.Uint32(record[envelopeHeaderSize+int(h.payloadLen):])
	if crc32.Checksum(payload, crcTable) != gotCRC {
		return nil, errors.New("journal: bad payload CRC")
	}
	return payload, nil
}
