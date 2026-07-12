package tape

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
)

// Migrator moves committed, activated parts from the disk journal onto tape.
// It follows a strict, crash-safe state machine per part:
//
//	JOURNALED
//	 → COPIED_TO_UNCOMMITTED_SEGMENT   (bytes written, segment not yet sealed)
//	 → TAPE_SEGMENT_COMMITTED          (footer + commit + filemark on tape)
//	 → TAPE_ACTIVATED                  (activation captured in the segment)
//	 → DISK_COPY_RECLAIMABLE           (MIGRATION_CHECKPOINT in the journal)
//
// The journal copy is never dropped before the tape copy is proven durable, so
// every crash window fails safe: an uncommitted segment is ignored on recovery
// and the part is migrated again; a committed-but-uncheckpointed part exists in
// both places and deduplicates by generation; a checkpointed part's journal
// copy is reclaimed later by compaction.
type Migrator struct {
	journal    *journal.Journal
	device     tapedev.Device
	recordSize int
	policy     SegmentPackingPolicy

	mu              sync.Mutex
	previousSegment [16]byte
	nextSequence    uint64
}

func NewMigrator(j *journal.Journal, device tapedev.Device, recordSize int, policy SegmentPackingPolicy, previousSegment [16]byte, nextSequence uint64) *Migrator {
	if recordSize <= 0 {
		recordSize = defaultRecordSize
	}
	return &Migrator{
		journal:         j,
		device:          device,
		recordSize:      recordSize,
		policy:          policy,
		previousSegment: previousSegment,
		nextSequence:    nextSequence,
	}
}

// MigratedPart records where a part landed on tape after migration.
type MigratedPart struct {
	PartID     partstore.PartId
	Generation journal.GenerationID
	StartBlock uint64
	Length     uint64
	Hash       [32]byte
}

// MigrationResult is the outcome of one MigrateOnce call.
type MigrationResult struct {
	SegmentID [16]byte
	Parts     []MigratedPart
}

// PreviousSegment returns the id of the last segment this migrator committed
// (or the seed value if none), for chain linkage.
func (m *Migrator) PreviousSegment() [16]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.previousSegment
}

// MigrateOnce plans and, if the plan is ready (or force is set), writes one
// tape segment from the journal's live, committed, not-yet-checkpointed parts.
// The returned result lists the migrated parts and their tape blocks; Parts is
// empty when there was nothing ready to migrate.
func (m *Migrator) MigrateOnce(ctx context.Context, force bool) (MigrationResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap, err := m.journal.Snapshot()
	if err != nil {
		return MigrationResult{}, err
	}

	candidates, byGen, activateSeqByGen := m.buildCandidates(snap)
	if len(candidates) == 0 {
		return MigrationResult{}, nil
	}
	plan := PlanSegment(candidates, m.policy)
	if len(plan.Parts) == 0 {
		return MigrationResult{}, nil
	}
	if !plan.Ready && !force {
		return MigrationResult{}, nil
	}

	writer, err := NewSegmentWriter(ctx, m.device, m.recordSize, m.previousSegment, m.nextSequence)
	if err != nil {
		return MigrationResult{}, err
	}

	migrated := make([]MigratedPart, 0, len(plan.Parts))
	for _, cand := range plan.Parts {
		part := byGen[cand.Generation]
		reader, err := m.journal.OpenPayload(part.Location)
		if err != nil {
			return MigrationResult{}, fmt.Errorf("opening journal payload for part %s: %w", part.PartID.String(), err)
		}
		meta := segPartBeginPayload{
			generation: part.Generation,
			partID:     part.PartID,
			objectID:   part.ObjectID,
			partNumber: part.PartNumber,
		}
		startBlock, writeErr := writer.WritePart(ctx, meta, reader)
		closeErr := reader.Close()
		if writeErr != nil {
			return MigrationResult{}, fmt.Errorf("writing part %s to tape: %w", part.PartID.String(), writeErr)
		}
		if closeErr != nil {
			return MigrationResult{}, closeErr
		}
		writer.AddActivate(segActivatePayload{
			partID:     part.PartID,
			generation: part.Generation,
			sequence:   activateSeqByGen[part.Generation],
		})
		migrated = append(migrated, MigratedPart{
			PartID:     part.PartID,
			Generation: part.Generation,
			StartBlock: startBlock,
			Length:     part.Length,
			Hash:       part.Hash,
		})
	}

	segmentID, err := writer.Finish(ctx)
	if err != nil {
		// The segment is not committed; recovery ignores it and the parts stay
		// authoritative in the journal.
		return MigrationResult{}, fmt.Errorf("committing tape segment: %w", err)
	}

	// The segment is durable on tape. Checkpoint each part so its journal copy
	// becomes reclaimable. A crash here leaves both copies, which recovery
	// deduplicates by generation.
	for _, p := range migrated {
		if _, err := m.journal.Checkpoint(ctx, p.Generation, segmentID); err != nil {
			return MigrationResult{}, fmt.Errorf("checkpointing migrated part: %w", err)
		}
	}

	m.previousSegment = segmentID
	m.nextSequence += uint64(len(plan.Parts))*4 + 8 // rough per-segment record span
	return MigrationResult{SegmentID: segmentID, Parts: migrated}, nil
}

// buildCandidates selects live parts whose payload is committed in the journal
// and not yet checkpointed onto tape.
func (m *Migrator) buildCandidates(snap *journal.RecoveryResult) ([]PackCandidate, map[journal.GenerationID]*journal.RecoveredPart, map[journal.GenerationID]uint64) {
	activateSeqByGen := map[journal.GenerationID]uint64{}
	for _, op := range snap.Ops {
		if op.IsActivate() {
			activateSeqByGen[op.Generation] = op.Sequence
		}
	}

	byGen := map[journal.GenerationID]*journal.RecoveredPart{}
	var candidates []PackCandidate
	now := time.Now()
	for partID, gen := range snap.Live {
		part, ok := snap.Parts[gen]
		if !ok || part.Checkpointed {
			continue
		}
		byGen[gen] = part
		candidates = append(candidates, PackCandidate{
			PartID:     partID,
			Generation: gen,
			Length:     part.Length,
			ObjectID:   part.ObjectID,
			PartNumber: part.PartNumber,
			PartCount:  part.PartCount,
			Arrival:    activateSeqByGen[gen],
			Age:        now.Sub(partID.CreatedAt()),
		})
	}
	return candidates, byGen, activateSeqByGen
}
