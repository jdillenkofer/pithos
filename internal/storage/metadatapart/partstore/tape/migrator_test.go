package tape

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
	"github.com/stretchr/testify/require"
)

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
	ctx := context.Background()
	j := openJournal(t)
	dev := openSimulator(t)

	id1, gen1 := stageActivePart(t, j, bytes.Repeat([]byte("one"), 1000))
	id2, gen2 := stageActivePart(t, j, []byte("two"))

	m := NewMigrator(j, dev, 4096, migrationTestPolicy(), [16]byte{}, 1)
	n, err := m.MigrateOnce(ctx, true)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	// The tape holds one committed segment with both parts.
	segments, err := scanSegments(ctx, dev)
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
	ctx := context.Background()
	j := openJournal(t)
	dev := openSimulator(t)
	stageActivePart(t, j, []byte("data"))

	m := NewMigrator(j, dev, 4096, migrationTestPolicy(), [16]byte{}, 1)
	n, err := m.MigrateOnce(ctx, true)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// A second run finds nothing left to migrate (the part is checkpointed).
	n, err = m.MigrateOnce(ctx, true)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	segments, err := scanSegments(ctx, dev)
	require.NoError(t, err)
	require.Len(t, segments, 1)
}

// TestMigrateCrashBeforeSegmentCommit reproduces a crash after part bytes are
// on tape but before the segment is sealed: the journal copy stays
// authoritative and the tape shows no trusted segment.
func TestMigrateCrashBeforeSegmentCommit(t *testing.T) {
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
	segments, err := scanSegments(ctx, dev)
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
	segments, err := scanSegments(ctx, dev)
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
