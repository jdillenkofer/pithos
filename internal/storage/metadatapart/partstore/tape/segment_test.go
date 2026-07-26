package tape

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
	"github.com/jdillenkofer/pithos/internal/tape/simulator"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type recordKindCountingDevice struct {
	tapedev.Device
	readKinds map[uint8]int
}

func (d *recordKindCountingDevice) ReadRecord(ctx context.Context, p []byte) (int, error) {
	n, err := d.Device.ReadRecord(ctx, p)
	if err == nil {
		if record, decodeErr := decodeSegmentRecord(p[:n]); decodeErr == nil {
			d.readKinds[record.kind]++
		}
	}
	return n, err
}

func openSimulator(t *testing.T) tapedev.Device {
	t.Helper()
	ctx := context.Background()
	dev, err := simulator.Open(ctx, filepath.Join(t.TempDir(), "tape.sim"), simulator.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dev.Close() })
	return dev
}

func segPartID(t *testing.T) partstore.PartId {
	t.Helper()
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	return *id
}

func segGen(t *testing.T) journal.GenerationID {
	t.Helper()
	g, err := journal.NewGenerationID()
	require.NoError(t, err)
	return g
}

// readPartAt reconstructs a part's payload by reading its data records from
// its part-begin block up to the part-end record.
func readPartAt(t *testing.T, dev tapedev.Device, block uint64) []byte {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, dev.LocateBlock(ctx, block))
	buf := make([]byte, 2<<20)
	// part-begin
	n, err := dev.ReadRecord(ctx, buf)
	require.NoError(t, err)
	rec, err := decodeSegmentRecord(buf[:n])
	require.NoError(t, err)
	require.Equal(t, uint8(segKindPartBegin), rec.kind)
	begin, err := decodeSegPartBegin(rec.payload)
	require.NoError(t, err)

	var out []byte
	for {
		n, err := dev.ReadRecord(ctx, buf)
		require.NoError(t, err)
		rec, err := decodeSegmentRecord(buf[:n])
		require.NoError(t, err)
		if rec.kind == segKindPartEnd {
			end, err := decodeSegPartEnd(rec.payload)
			require.NoError(t, err)
			require.Equal(t, begin.generation, end.generation)
			require.Equal(t, uint64(len(out)), end.dataLength)
			break
		}
		require.Equal(t, uint8(segKindPartData), rec.kind)
		out = append(out, rec.payload...)
	}
	return out
}

func TestSegmentWriteAndScanRoundtrip(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dev := openSimulator(t)

	w, err := NewSegmentWriter(ctx, dev, 4096, [16]byte{}, 1)
	require.NoError(t, err)

	type part struct {
		id    partstore.PartId
		gen   journal.GenerationID
		data  []byte
		block uint64
	}
	parts := []part{
		{id: segPartID(t), gen: segGen(t), data: bytes.Repeat([]byte("alpha"), 3000)},
		{id: segPartID(t), gen: segGen(t), data: []byte("small")},
		{id: segPartID(t), gen: segGen(t), data: nil}, // empty part
	}
	for i := range parts {
		block, err := w.WritePart(ctx, segPartBeginPayload{generation: parts[i].gen, partID: parts[i].id}, bytes.NewReader(parts[i].data))
		require.NoError(t, err)
		parts[i].block = block
	}
	w.AddActivate(segActivatePayload{partID: parts[0].id, generation: parts[0].gen, sequence: 10})
	w.AddDelete(segDeletePayload{partID: parts[1].id, sequence: 11})
	segmentID, err := w.Finish(ctx)
	require.NoError(t, err)

	segments, _, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	seg := segments[0]
	require.Equal(t, segmentID, seg.header.segmentID)
	require.Equal(t, uint64(3), seg.footer.partCount)
	require.Equal(t, uint64(2), seg.footer.logicalOpCount)
	require.Len(t, seg.index, 3)
	require.Len(t, seg.activates, 1)
	require.Len(t, seg.deletes, 1)

	// Index entries carry the block of each part-begin; read data back.
	byGen := map[journal.GenerationID]segIndexEntry{}
	for _, e := range seg.index {
		byGen[e.generation] = e
	}
	for _, p := range parts {
		entry, ok := byGen[p.gen]
		require.True(t, ok)
		require.Equal(t, p.block, entry.startBlock)
		require.Equal(t, uint64(len(p.data)), entry.dataLength)
		got := readPartAt(t, dev, entry.startBlock)
		if len(p.data) == 0 {
			require.Empty(t, got)
		} else {
			require.Equal(t, p.data, got)
		}
	}
}

func TestSegmentScanSpacesOverPayloadRecords(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	base := openSimulator(t)
	w, err := NewSegmentWriter(ctx, base, 1024, [16]byte{}, 1)
	require.NoError(t, err)
	data := bytes.Repeat([]byte("bulk"), 2<<20)
	_, err = w.WritePart(ctx, segPartBeginPayload{generation: segGen(t), partID: segPartID(t)}, bytes.NewReader(data))
	require.NoError(t, err)
	_, err = w.Finish(ctx)
	require.NoError(t, err)

	counting := &recordKindCountingDevice{Device: base, readKinds: make(map[uint8]int)}
	segments, _, err := scanSegments(ctx, counting)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Zero(t, counting.readKinds[segKindPartData])
	require.Equal(t, 1, counting.readKinds[segKindHeader])
	require.Equal(t, 1, counting.readKinds[segKindIndexChunk])
}

func TestSegmentChainScan(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dev := openSimulator(t)

	w1, err := NewSegmentWriter(ctx, dev, 4096, [16]byte{}, 1)
	require.NoError(t, err)
	_, err = w1.WritePart(ctx, segPartBeginPayload{generation: segGen(t), partID: segPartID(t)}, bytes.NewReader([]byte("one")))
	require.NoError(t, err)
	seg1ID, err := w1.Finish(ctx)
	require.NoError(t, err)

	w2, err := NewSegmentWriter(ctx, dev, 4096, seg1ID, 100)
	require.NoError(t, err)
	_, err = w2.WritePart(ctx, segPartBeginPayload{generation: segGen(t), partID: segPartID(t)}, bytes.NewReader([]byte("two")))
	require.NoError(t, err)
	seg2ID, err := w2.Finish(ctx)
	require.NoError(t, err)

	segments, _, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Len(t, segments, 2)
	require.Equal(t, seg1ID, segments[0].header.segmentID)
	require.Equal(t, seg2ID, segments[1].header.segmentID)
	// Chain linkage: segment 2 references segment 1.
	require.Equal(t, seg1ID, segments[1].header.previousSegment)
}

func TestSegmentScanIgnoresTornTailSegment(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dev := openSimulator(t)

	w, err := NewSegmentWriter(ctx, dev, 4096, [16]byte{}, 1)
	require.NoError(t, err)
	_, err = w.WritePart(ctx, segPartBeginPayload{generation: segGen(t), partID: segPartID(t)}, bytes.NewReader([]byte("committed")))
	require.NoError(t, err)
	seg1ID, err := w.Finish(ctx)
	require.NoError(t, err)

	// Simulate a crash mid-way through a second segment: a header and a
	// part-begin, but no footer/commit/filemark.
	require.NoError(t, dev.SeekToEOD(ctx))
	hdr, err := encodeSegmentRecord(segKindHeader, 100, encodeSegmentHeader(segmentHeaderPayload{segmentID: [16]byte{2}, previousSegment: seg1ID, sequenceStart: 100}))
	require.NoError(t, err)
	require.NoError(t, dev.WriteRecord(ctx, hdr))
	pb, err := encodeSegmentRecord(segKindPartBegin, 101, encodeSegPartBegin(segPartBeginPayload{generation: segGen(t), partID: segPartID(t)}))
	require.NoError(t, err)
	require.NoError(t, dev.WriteRecord(ctx, pb))

	segments, _, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Equal(t, seg1ID, segments[0].header.segmentID)
}

func TestSegmentScanRejectsFooterWithoutCommit(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dev := openSimulator(t)

	// Write a header, a part, and a footer, then a filemark — but no commit
	// record. The segment must not be trusted.
	w, err := NewSegmentWriter(ctx, dev, 4096, [16]byte{}, 1)
	require.NoError(t, err)
	_, err = w.WritePart(ctx, segPartBeginPayload{generation: segGen(t), partID: segPartID(t)}, bytes.NewReader([]byte("x")))
	require.NoError(t, err)
	// Manually emulate finishing without a commit: footer then filemark.
	footer := segmentFooterPayload{segmentID: w.SegmentID(), recordCount: w.recordCount, partCount: 1, indexStartBlock: 0}
	rec, err := encodeSegmentRecord(segKindFooter, w.seq, encodeSegmentFooter(footer))
	require.NoError(t, err)
	require.NoError(t, dev.WriteRecord(ctx, rec))
	require.NoError(t, dev.WriteFilemarks(ctx, 1))

	segments, _, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Empty(t, segments)
}

func TestSegmentScanEmptyTape(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dev := openSimulator(t)
	segments, _, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Empty(t, segments)
}

var _ = io.EOF
