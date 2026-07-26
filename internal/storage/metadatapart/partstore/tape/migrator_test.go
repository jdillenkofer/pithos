package tape

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type failRecordOnceDevice struct {
	tapedev.Device
	failAt int
	writes int
	failed bool
	err    error
}

func (d *failRecordOnceDevice) WriteRecord(ctx context.Context, p []byte) error {
	d.writes++
	if !d.failed && d.writes == d.failAt {
		d.failed = true
		return d.err
	}
	return d.Device.WriteRecord(ctx, p)
}

func openJournal(t *testing.T) *journal.Journal {
	t.Helper()
	j, err := journal.Open(journal.Options{Dir: filepath.Join(t.TempDir(), "journal")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = j.Close() })
	return j
}

func stageActivePart(t *testing.T, j *journal.Journal, data []byte) (partstore.PartId, journal.GenerationID) {
	t.Helper()
	ctx := context.Background()
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	gen, err := journal.NewGenerationID()
	require.NoError(t, err)
	_, err = j.WritePart(ctx, journal.PartInput{Generation: gen, PartID: *id}, bytes.NewReader(data))
	require.NoError(t, err)
	_, err = j.Activate(ctx, *id, gen, nil)
	require.NoError(t, err)
	return *id, gen
}

func migrationTestPolicy() SegmentPackingPolicy {
	p := DefaultPackingPolicy()
	p.TargetBytes = 1
	p.MaxWait = time.Hour
	p.PreferFullObject = false
	return p
}

func TestMigrateOnceHappyPath(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	j := openJournal(t)
	dev := openSimulator(t)

	id1, gen1 := stageActivePart(t, j, bytes.Repeat([]byte("one"), 1000))
	id2, gen2 := stageActivePart(t, j, []byte("two"))

	m := NewMigrator(j, dev, 4096, migrationTestPolicy(), [16]byte{}, 1)
	res, err := m.MigrateOnce(ctx, true)
	require.NoError(t, err)
	require.Len(t, res.Parts, 2)
	for _, part := range res.Parts {
		_, err := j.Checkpoint(ctx, part.Generation, res.SegmentID)
		require.NoError(t, err)
	}

	// The tape holds one committed segment with both parts.
	segments, _, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Equal(t, uint64(2), segments[0].footer.partCount)

	byGen := map[journal.GenerationID]segIndexEntry{}
	for _, e := range segments[0].index {
		byGen[e.generation] = e
	}
	require.Contains(t, byGen, gen1)
	require.Contains(t, byGen, gen2)
	require.Equal(t, bytes.Repeat([]byte("one"), 1000), readPartAt(t, dev, byGen[gen1].startBlock))
	require.Equal(t, []byte("two"), readPartAt(t, dev, byGen[gen2].startBlock))

	// Both parts are now checkpointed in the journal (disk copy reclaimable).
	snap, err := j.Snapshot()
	require.NoError(t, err)
	require.True(t, snap.Parts[gen1].Checkpointed)
	require.True(t, snap.Parts[gen2].Checkpointed)
	require.Equal(t, segments[0].header.segmentID, snap.Parts[gen1].CheckpointSegment)
	_ = id1
	_ = id2
}

func TestMigrateAlreadyCheckpointedNotRemigrated(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	j := openJournal(t)
	dev := openSimulator(t)
	stageActivePart(t, j, []byte("data"))

	m := NewMigrator(j, dev, 4096, migrationTestPolicy(), [16]byte{}, 1)
	res, err := m.MigrateOnce(ctx, true)
	require.NoError(t, err)
	require.Len(t, res.Parts, 1)
	for _, part := range res.Parts {
		_, err := j.Checkpoint(ctx, part.Generation, res.SegmentID)
		require.NoError(t, err)
	}

	// A second run finds nothing left to migrate (the part is checkpointed).
	res, err = m.MigrateOnce(ctx, true)
	require.NoError(t, err)
	require.Empty(t, res.Parts)

	segments, _, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Len(t, segments, 1)
}

func TestMigrateRetrySealsFailedSegmentTail(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	j := openJournal(t)
	base := openSimulator(t)
	partID, generation := stageActivePart(t, j, []byte("payload"))
	writeErr := errors.New("injected tape write failure")
	dev := &failRecordOnceDevice{Device: base, failAt: 2, err: writeErr}
	m := NewMigrator(j, dev, 4096, migrationTestPolicy(), [16]byte{}, 1)

	_, err := m.MigrateOnce(ctx, true)
	require.ErrorIs(t, err, writeErr)

	result, err := m.MigrateOnce(ctx, true)
	require.NoError(t, err)
	require.True(t, result.Committed)
	require.Len(t, result.Parts, 1)

	segments, _, err := scanSegments(ctx, base)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Equal(t, generation, segments[0].index[0].generation)
	require.Equal(t, partID, segments[0].index[0].partID)
}

func TestMigrateWritesMetadataOnlyDeletionSegment(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	j := openJournal(t)
	dev := openSimulator(t)
	partID, generation := stageActivePart(t, j, []byte("payload"))

	m := NewMigrator(j, dev, 4096, migrationTestPolicy(), [16]byte{}, 1)
	first, err := m.MigrateOnce(ctx, true)
	require.NoError(t, err)
	require.True(t, first.Committed)
	require.Len(t, first.Parts, 1)
	_, err = j.Checkpoint(ctx, generation, first.SegmentID)
	require.NoError(t, err)

	_, err = j.Delete(ctx, partID, &generation)
	require.NoError(t, err)
	second, err := m.MigrateOnce(ctx, true)
	require.NoError(t, err)
	require.True(t, second.Committed)
	require.Empty(t, second.Parts)

	segments, _, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Len(t, segments, 2)
	require.Len(t, segments[1].deletes, 1)
	require.True(t, segments[1].deletes[0].partID.Equal(partID))
	require.Equal(t, &generation, segments[1].deletes[0].expectedGeneration)
}

// TestMigrateCrashBeforeSegmentCommit reproduces a crash after part bytes are
// on tape but before the segment is sealed: the journal copy stays
// authoritative and the tape shows no trusted segment.
func TestMigrateCrashBeforeSegmentCommit(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	j := openJournal(t)
	dev := openSimulator(t)
	_, gen := stageActivePart(t, j, []byte("payload"))

	// Write the segment bytes but never call Finish (crash before commit).
	w, err := NewSegmentWriter(ctx, dev, 4096, [16]byte{}, 1)
	require.NoError(t, err)
	snap, err := j.Snapshot()
	require.NoError(t, err)
	part := snap.Parts[gen]
	reader, err := j.OpenPayload(part.Location)
	require.NoError(t, err)
	_, err = w.WritePart(ctx, segPartBeginPayload{generation: part.Generation, partID: part.PartID}, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	// No trusted segment exists on tape.
	segments, _, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Empty(t, segments)

	// The journal copy is still live and not checkpointed.
	snap, err = j.Snapshot()
	require.NoError(t, err)
	require.Contains(t, snap.Live, part.PartID)
	require.False(t, snap.Parts[gen].Checkpointed)
}

// TestMigrateCrashAfterCommitBeforeCheckpoint reproduces a crash after the
// segment is committed on tape but before the journal checkpoint: both copies
// exist and can be deduplicated by generation.
func TestMigrateCrashAfterCommitBeforeCheckpoint(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	j := openJournal(t)
	dev := openSimulator(t)
	id, gen := stageActivePart(t, j, []byte("payload"))

	// Commit a segment for the part but skip the journal checkpoint.
	w, err := NewSegmentWriter(ctx, dev, 4096, [16]byte{}, 1)
	require.NoError(t, err)
	snap, err := j.Snapshot()
	require.NoError(t, err)
	part := snap.Parts[gen]
	reader, err := j.OpenPayload(part.Location)
	require.NoError(t, err)
	_, err = w.WritePart(ctx, segPartBeginPayload{generation: part.Generation, partID: part.PartID}, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	w.AddActivate(segActivatePayload{partID: id, generation: gen, sequence: 1})
	segmentID, err := w.Finish(ctx)
	require.NoError(t, err)

	// Tape copy exists and is trusted.
	segments, _, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Equal(t, segmentID, segments[0].header.segmentID)
	require.Equal(t, gen, segments[0].index[0].generation)

	// Journal copy also exists (not checkpointed): the same generation is in
	// both places, so recovery can safely deduplicate.
	snap, err = j.Snapshot()
	require.NoError(t, err)
	require.False(t, snap.Parts[gen].Checkpointed)
	require.Equal(t, gen, snap.Live[id])
}
