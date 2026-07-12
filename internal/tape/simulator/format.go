package simulator

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// On-disk format of a simulated tape file:
//
//	header (32 bytes):
//	  magic    [8]byte  "PTAPESIM"
//	  version  uint32   little-endian
//	  reserved uint32
//	  capacity uint64   payload capacity in bytes, 0 = unlimited
//	  reserved uint64
//
//	body: sequence of entries
//	  record:   [len uint32][payload len bytes][len uint32]
//	  filemark: [0xFFFFFFFF][0xFFFFFFFF]
//
// The trailing length copy makes entries traversable backward. End-of-data
// is implicit: it is the end of the file.
const (
	headerMagic    = "PTAPESIM"
	headerVersion  = 1
	headerSize     = 32
	filemarkMarker = uint32(0xFFFFFFFF)
	// maxRecordSize keeps record lengths distinguishable from filemarkMarker.
	maxRecordSize = 1 << 30
)

type entryKind uint8

const (
	entryRecord entryKind = iota
	entryFilemark
)

type entry struct {
	// offset is the file offset of the entry start.
	offset int64
	// size is the payload size for records, 0 for filemarks.
	size uint32
	kind entryKind
}

func (e entry) byteLen() int64 {
	if e.kind == entryFilemark {
		return 8
	}
	return 8 + int64(e.size)
}

func writeHeader(f *os.File, capacity uint64) error {
	var h [headerSize]byte
	copy(h[0:8], headerMagic)
	binary.LittleEndian.PutUint32(h[8:12], headerVersion)
	binary.LittleEndian.PutUint64(h[16:24], capacity)
	_, err := f.WriteAt(h[:], 0)
	return err
}

func readHeader(f *os.File) (capacity uint64, err error) {
	var h [headerSize]byte
	if _, err := f.ReadAt(h[:], 0); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, errors.New("tape file too small to contain a header")
		}
		return 0, fmt.Errorf("reading tape file header: %w", err)
	}
	if string(h[0:8]) != headerMagic {
		return 0, errors.New("invalid tape file magic")
	}
	if version := binary.LittleEndian.Uint32(h[8:12]); version != headerVersion {
		return 0, fmt.Errorf("unsupported tape file version %d", version)
	}
	return binary.LittleEndian.Uint64(h[16:24]), nil
}

// scanIndex rebuilds the entry index by walking the framing from the start
// of the body. A torn or corrupt tail is treated as end-of-data, matching a
// torn tape write; the next write truncates it away.
func scanIndex(f *os.File, fileSize int64) (index []entry, payloadBytes int64) {
	offset := int64(headerSize)
	var lenBuf [4]byte
	for offset+8 <= fileSize {
		if _, err := f.ReadAt(lenBuf[:], offset); err != nil {
			break
		}
		lead := binary.LittleEndian.Uint32(lenBuf[:])
		if lead == filemarkMarker {
			if _, err := f.ReadAt(lenBuf[:], offset+4); err != nil {
				break
			}
			if binary.LittleEndian.Uint32(lenBuf[:]) != filemarkMarker {
				break
			}
			index = append(index, entry{offset: offset, kind: entryFilemark})
			offset += 8
			continue
		}
		if lead > maxRecordSize || offset+8+int64(lead) > fileSize {
			break
		}
		if _, err := f.ReadAt(lenBuf[:], offset+4+int64(lead)); err != nil {
			break
		}
		if binary.LittleEndian.Uint32(lenBuf[:]) != lead {
			break
		}
		index = append(index, entry{offset: offset, size: lead, kind: entryRecord})
		payloadBytes += int64(lead)
		offset += 8 + int64(lead)
	}
	return index, payloadBytes
}
