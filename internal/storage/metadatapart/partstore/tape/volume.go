package tape

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
)

const (
	volumeLabelMagic   = "PTVL"
	volumeLabelVersion = 1
	maxVolumeIDBytes   = 128
	catalogFileName    = "tape-catalog.pcat"
	catalogMagic       = "PTC1"
	maxCatalogBytes    = 256 << 20
)

var ErrWrongTapeMedia = errors.New("tape: catalog belongs to different physical media")

// volumeLabel is permanently written at BOT. mediaID distinguishes a new
// cartridge that was accidentally assigned an existing operator-facing ID.
type volumeLabel struct {
	VolumeID  string
	MediaID   [16]byte
	CreatedAt int64
}

func encodeVolumeLabel(label volumeLabel) ([]byte, error) {
	if label.VolumeID == "" || len(label.VolumeID) > maxVolumeIDBytes {
		return nil, fmt.Errorf("tape volume id must contain 1..%d bytes", maxVolumeIDBytes)
	}
	buf := make([]byte, 4+1+2+len(label.VolumeID)+16+8+4)
	copy(buf[0:4], volumeLabelMagic)
	buf[4] = volumeLabelVersion
	binary.BigEndian.PutUint16(buf[5:7], uint16(len(label.VolumeID)))
	copy(buf[7:], label.VolumeID)
	off := 7 + len(label.VolumeID)
	copy(buf[off:off+16], label.MediaID[:])
	binary.BigEndian.PutUint64(buf[off+16:off+24], uint64(label.CreatedAt))
	binary.BigEndian.PutUint32(buf[off+24:off+28], crc32.Checksum(buf[:off+24], segCRCTable))
	return buf, nil
}

func decodeVolumeLabel(buf []byte) (volumeLabel, error) {
	if len(buf) < 4+1+2+16+8+4 || string(buf[:4]) != volumeLabelMagic {
		return volumeLabel{}, errors.New("tape: missing volume label")
	}
	if buf[4] != volumeLabelVersion {
		return volumeLabel{}, fmt.Errorf("tape: unsupported volume label version %d", buf[4])
	}
	idLen := int(binary.BigEndian.Uint16(buf[5:7]))
	if idLen == 0 || idLen > maxVolumeIDBytes || len(buf) != 4+1+2+idLen+16+8+4 {
		return volumeLabel{}, errors.New("tape: malformed volume label")
	}
	off := 7 + idLen
	if binary.BigEndian.Uint32(buf[off+24:off+28]) != crc32.Checksum(buf[:off+24], segCRCTable) {
		return volumeLabel{}, errors.New("tape: corrupt volume label")
	}
	var label volumeLabel
	label.VolumeID = string(buf[7:off])
	copy(label.MediaID[:], buf[off:off+16])
	label.CreatedAt = int64(binary.BigEndian.Uint64(buf[off+16 : off+24]))
	return label, nil
}

// openVolume validates an existing label or initializes an empty writable
// cartridge. It returns the first segment block immediately after the label
// filemark.
func openVolume(ctx context.Context, dev tapedev.Device, configuredID string) (volumeLabel, uint64, error) {
	if configuredID == "" {
		return volumeLabel{}, 0, errors.New("tape: volume id is required")
	}
	if err := dev.Rewind(ctx); err != nil {
		return volumeLabel{}, 0, err
	}
	buf := make([]byte, 512)
	n, err := dev.ReadRecord(ctx, buf)
	if errors.Is(err, tapedev.ErrEndOfData) {
		label := volumeLabel{VolumeID: configuredID, CreatedAt: time.Now().UnixNano()}
		if _, err := rand.Read(label.MediaID[:]); err != nil {
			return volumeLabel{}, 0, err
		}
		encoded, err := encodeVolumeLabel(label)
		if err != nil {
			return volumeLabel{}, 0, err
		}
		if err := dev.WriteRecord(ctx, encoded); err != nil {
			return volumeLabel{}, 0, err
		}
		if err := dev.WriteFilemarks(ctx, 1); err != nil {
			return volumeLabel{}, 0, err
		}
		if err := dev.Flush(ctx); err != nil {
			return volumeLabel{}, 0, err
		}
		pos, err := dev.Tell(ctx)
		return label, pos.Block, err
	}
	if err != nil {
		return volumeLabel{}, 0, err
	}
	label, err := decodeVolumeLabel(buf[:n])
	if err != nil {
		return volumeLabel{}, 0, err
	}
	if label.VolumeID != configuredID {
		return volumeLabel{}, 0, fmt.Errorf("tape: mounted volume %q, expected %q", label.VolumeID, configuredID)
	}
	n, err = dev.ReadRecord(ctx, buf)
	if !errors.Is(err, tapedev.ErrFilemark) {
		if err == nil {
			return volumeLabel{}, 0, errors.New("tape: volume label is not terminated by a filemark")
		}
		return volumeLabel{}, 0, err
	}
	pos, err := dev.Tell(ctx)
	return label, pos.Block, err
}

type catalogIndexEntry struct {
	Generation [16]byte
	PartID     [16]byte
	StartBlock uint64
	DataLength uint64
	DataHash   [32]byte
}

type catalogActivate struct {
	PartID              [16]byte
	Generation          [16]byte
	HasExpectedPrevious bool
	ExpectedPrevious    [16]byte
	Sequence            uint64
}

type catalogDelete struct {
	PartID                [16]byte
	HasExpectedGeneration bool
	ExpectedGeneration    [16]byte
	Sequence              uint64
}

type catalogSegment struct {
	SegmentID       [16]byte
	CreatedUnixNano int64
	PreviousSegment [16]byte
	SequenceStart   uint64
	RecordCount     uint64
	PartCount       uint64
	LogicalOpCount  uint64
	IndexStartBlock uint64
	ByteCount       uint64
	NextSequence    uint64
	ContentHash     [32]byte
	FirstBlock      uint64
	EndBlock        uint64
	Index           []catalogIndexEntry
	Activates       []catalogActivate
	Deletes         []catalogDelete
}

type tapeCatalog struct {
	Version         uint32
	VolumeID        string
	MediaID         [16]byte
	DataStartBlock  uint64
	TailBlock       uint64
	PreviousSegment [16]byte
	Segments        []catalogSegment
}

func newTapeCatalog(label volumeLabel, dataStart uint64) *tapeCatalog {
	return &tapeCatalog{
		Version:        1,
		VolumeID:       label.VolumeID,
		MediaID:        label.MediaID,
		DataStartBlock: dataStart,
		TailBlock:      dataStart,
	}
}

func (c *tapeCatalog) appendSegment(seg scannedSegment) {
	cs := catalogSegment{
		SegmentID:       seg.header.segmentID,
		CreatedUnixNano: seg.header.createdUnixNano,
		PreviousSegment: seg.header.previousSegment,
		SequenceStart:   seg.header.sequenceStart,
		RecordCount:     seg.footer.recordCount,
		PartCount:       seg.footer.partCount,
		LogicalOpCount:  seg.footer.logicalOpCount,
		IndexStartBlock: seg.footer.indexStartBlock,
		ByteCount:       seg.footer.segmentByteCount,
		NextSequence:    seg.footer.nextSequence,
		ContentHash:     seg.footer.contentHash,
		FirstBlock:      seg.firstBlock,
		EndBlock:        seg.endBlock,
	}
	for _, e := range seg.index {
		var partID [16]byte
		copy(partID[:], e.partID.Bytes())
		cs.Index = append(cs.Index, catalogIndexEntry{
			Generation: e.generation,
			PartID:     partID,
			StartBlock: e.startBlock,
			DataLength: e.dataLength,
			DataHash:   e.dataHash,
		})
	}
	for _, a := range seg.activates {
		var partID [16]byte
		copy(partID[:], a.partID.Bytes())
		item := catalogActivate{PartID: partID, Generation: a.generation, Sequence: a.sequence}
		if a.expectedPrevious != nil {
			item.HasExpectedPrevious = true
			item.ExpectedPrevious = *a.expectedPrevious
		}
		cs.Activates = append(cs.Activates, item)
	}
	for _, d := range seg.deletes {
		var partID [16]byte
		copy(partID[:], d.partID.Bytes())
		item := catalogDelete{PartID: partID, Sequence: d.sequence}
		if d.expectedGeneration != nil {
			item.HasExpectedGeneration = true
			item.ExpectedGeneration = *d.expectedGeneration
		}
		cs.Deletes = append(cs.Deletes, item)
	}
	c.Segments = append(c.Segments, cs)
	c.PreviousSegment = seg.header.segmentID
	c.TailBlock = seg.endBlock
}

func (c *tapeCatalog) scannedSegments() ([]scannedSegment, error) {
	segments := make([]scannedSegment, 0, len(c.Segments))
	for _, cs := range c.Segments {
		seg := scannedSegment{
			header: segmentHeaderPayload{
				segmentID:       cs.SegmentID,
				createdUnixNano: cs.CreatedUnixNano,
				previousSegment: cs.PreviousSegment,
				sequenceStart:   cs.SequenceStart,
			},
			footer: segmentFooterPayload{
				segmentID:        cs.SegmentID,
				recordCount:      cs.RecordCount,
				partCount:        cs.PartCount,
				logicalOpCount:   cs.LogicalOpCount,
				indexStartBlock:  cs.IndexStartBlock,
				segmentByteCount: cs.ByteCount,
				nextSequence:     cs.NextSequence,
				contentHash:      cs.ContentHash,
			},
			firstBlock: cs.FirstBlock,
			endBlock:   cs.EndBlock,
		}
		for _, e := range cs.Index {
			partID, err := partstore.NewPartIdFromBytes(e.PartID[:])
			if err != nil {
				return nil, err
			}
			seg.index = append(seg.index, segIndexEntry{
				generation: journal.GenerationID(e.Generation),
				partID:     *partID,
				startBlock: e.StartBlock,
				dataLength: e.DataLength,
				dataHash:   e.DataHash,
			})
		}
		for _, a := range cs.Activates {
			partID, err := partstore.NewPartIdFromBytes(a.PartID[:])
			if err != nil {
				return nil, err
			}
			item := segActivatePayload{partID: *partID, generation: journal.GenerationID(a.Generation), sequence: a.Sequence}
			if a.HasExpectedPrevious {
				expected := journal.GenerationID(a.ExpectedPrevious)
				item.expectedPrevious = &expected
			}
			seg.activates = append(seg.activates, item)
		}
		for _, d := range cs.Deletes {
			partID, err := partstore.NewPartIdFromBytes(d.PartID[:])
			if err != nil {
				return nil, err
			}
			item := segDeletePayload{partID: *partID, sequence: d.Sequence}
			if d.HasExpectedGeneration {
				expected := journal.GenerationID(d.ExpectedGeneration)
				item.expectedGeneration = &expected
			}
			seg.deletes = append(seg.deletes, item)
		}
		segments = append(segments, seg)
	}
	return segments, nil
}

func loadTapeCatalog(dir string, label volumeLabel) (*tapeCatalog, error) {
	path := filepath.Join(dir, catalogFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) < 4+8+32 || string(data[:4]) != catalogMagic {
		return nil, errors.New("tape: invalid catalog envelope")
	}
	payloadLen := binary.BigEndian.Uint64(data[4:12])
	if payloadLen > maxCatalogBytes || int(payloadLen) != len(data)-44 {
		return nil, errors.New("tape: invalid catalog length")
	}
	wantHash := data[12:44]
	payload := data[44:]
	gotHash := sha256.Sum256(payload)
	if !bytes.Equal(wantHash, gotHash[:]) {
		return nil, errors.New("tape: catalog checksum mismatch")
	}
	var catalog tapeCatalog
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&catalog); err != nil {
		return nil, err
	}
	if catalog.Version != 1 {
		return nil, fmt.Errorf("tape: unsupported catalog version %d", catalog.Version)
	}
	if catalog.VolumeID != label.VolumeID || catalog.MediaID != label.MediaID {
		return nil, ErrWrongTapeMedia
	}
	if err := catalog.validate(); err != nil {
		return nil, err
	}
	return &catalog, nil
}

func (c *tapeCatalog) validate() error {
	if c.DataStartBlock > c.TailBlock {
		return errors.New("tape: catalog tail precedes data start")
	}
	previous := [16]byte{}
	minimumBlock := c.DataStartBlock
	var nextSequence uint64 = 1
	seenSegments := make(map[[16]byte]struct{}, len(c.Segments))
	for index, segment := range c.Segments {
		if segment.SegmentID == ([16]byte{}) {
			return fmt.Errorf("tape: catalog segment %d has an empty id", index)
		}
		if _, duplicate := seenSegments[segment.SegmentID]; duplicate {
			return fmt.Errorf("tape: catalog repeats segment %x", segment.SegmentID)
		}
		seenSegments[segment.SegmentID] = struct{}{}
		if segment.PreviousSegment != previous {
			return fmt.Errorf("tape: catalog segment %d has a broken chain", index)
		}
		if segment.SequenceStart != nextSequence || segment.NextSequence <= segment.SequenceStart {
			return fmt.Errorf("tape: catalog segment %d has an invalid record sequence range", index)
		}
		if segment.FirstBlock < minimumBlock || segment.FirstBlock >= segment.IndexStartBlock || segment.IndexStartBlock >= segment.EndBlock {
			return fmt.Errorf("tape: catalog segment %d has invalid block bounds", index)
		}
		if segment.PartCount != uint64(len(segment.Index)) ||
			segment.LogicalOpCount != uint64(len(segment.Activates)+len(segment.Deletes)) {
			return fmt.Errorf("tape: catalog segment %d has invalid manifest counts", index)
		}
		seenGenerations := make(map[[16]byte]struct{}, len(segment.Index))
		for _, entry := range segment.Index {
			if entry.StartBlock <= segment.FirstBlock || entry.StartBlock >= segment.IndexStartBlock {
				return fmt.Errorf("tape: catalog segment %d indexes block outside its data file", index)
			}
			if _, duplicate := seenGenerations[entry.Generation]; duplicate {
				return fmt.Errorf("tape: catalog segment %d repeats a generation", index)
			}
			seenGenerations[entry.Generation] = struct{}{}
		}
		previous = segment.SegmentID
		minimumBlock = segment.EndBlock
		nextSequence = segment.NextSequence
	}
	if c.PreviousSegment != previous {
		return errors.New("tape: catalog previous-segment pointer is invalid")
	}
	if len(c.Segments) == 0 {
		if c.TailBlock != c.DataStartBlock {
			return errors.New("tape: empty catalog has an invalid tail")
		}
	} else if c.TailBlock != c.Segments[len(c.Segments)-1].EndBlock {
		return errors.New("tape: catalog tail does not match its final segment")
	}
	return nil
}

func saveTapeCatalog(dir string, catalog *tapeCatalog) error {
	if err := catalog.validate(); err != nil {
		return err
	}
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(catalog); err != nil {
		return err
	}
	if payload.Len() > maxCatalogBytes {
		return fmt.Errorf("tape: catalog exceeds %d bytes", maxCatalogBytes)
	}
	envelope := make([]byte, 44+payload.Len())
	copy(envelope[:4], catalogMagic)
	binary.BigEndian.PutUint64(envelope[4:12], uint64(payload.Len()))
	sum := sha256.Sum256(payload.Bytes())
	copy(envelope[12:44], sum[:])
	copy(envelope[44:], payload.Bytes())

	tmp, err := os.CreateTemp(dir, ".tape-catalog-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(envelope); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, catalogFileName)); err != nil {
		return err
	}
	remove = false
	return ioutils.SyncDirectory(dir)
}
