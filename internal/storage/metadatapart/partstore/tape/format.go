package tape

import (
	"encoding/binary"
	"fmt"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// On-tape layout: everything is appended at end-of-data. Two segment shapes:
//
//	data segment:      [data header record] [data record]* [filemark]
//	tombstone segment: [tombstone record] [filemark]
//
// A data header carries the partId, making every part self-describing so the
// index can be rebuilt by scanning the tape. A tombstone invalidates either
// one data segment (identified by the block of its header record) or every
// copy of a part.
const (
	headerMagic   = "PTPS"
	headerVersion = 1

	kindData      = 1
	kindTombstone = 2

	dataHeaderSize = 22
	tombstoneSize  = 30

	// headerBufferSize is the read buffer for header records; any record
	// larger than this is not a header (io.ErrShortBuffer on read).
	headerBufferSize = 64

	// tombstoneAllCopies invalidates every copy of a part.
	tombstoneAllCopies = ^uint64(0)
)

type header struct {
	kind   byte
	partId partstore.PartId
	// invalidatedBlock is only meaningful for tombstones: the block of the
	// data header record it invalidates, or tombstoneAllCopies.
	invalidatedBlock uint64
}

func encodeDataHeader(partId partstore.PartId) []byte {
	buf := make([]byte, dataHeaderSize)
	copy(buf[0:4], headerMagic)
	buf[4] = headerVersion
	buf[5] = kindData
	copy(buf[6:22], partId.Bytes())
	return buf
}

func encodeTombstone(partId partstore.PartId, invalidatedBlock uint64) []byte {
	buf := make([]byte, tombstoneSize)
	copy(buf[0:4], headerMagic)
	buf[4] = headerVersion
	buf[5] = kindTombstone
	copy(buf[6:22], partId.Bytes())
	binary.BigEndian.PutUint64(buf[22:30], invalidatedBlock)
	return buf
}

func decodeHeader(buf []byte) (*header, error) {
	if len(buf) < dataHeaderSize {
		return nil, fmt.Errorf("record too short for a header: %d bytes", len(buf))
	}
	if string(buf[0:4]) != headerMagic {
		return nil, fmt.Errorf("invalid header magic")
	}
	if buf[4] != headerVersion {
		return nil, fmt.Errorf("unsupported header version %d", buf[4])
	}
	partId, err := partstore.NewPartIdFromBytes(buf[6:22])
	if err != nil {
		return nil, fmt.Errorf("invalid partId in header: %w", err)
	}
	h := &header{kind: buf[5], partId: *partId}
	switch h.kind {
	case kindData:
		if len(buf) != dataHeaderSize {
			return nil, fmt.Errorf("invalid data header length %d", len(buf))
		}
	case kindTombstone:
		if len(buf) != tombstoneSize {
			return nil, fmt.Errorf("invalid tombstone length %d", len(buf))
		}
		h.invalidatedBlock = binary.BigEndian.Uint64(buf[22:30])
	default:
		return nil, fmt.Errorf("unknown header kind %d", h.kind)
	}
	return h, nil
}
