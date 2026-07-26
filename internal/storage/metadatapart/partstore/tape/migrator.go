package tape

import (
	"context"
	"fmt"
	"sort"
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
	capturedOps     map[partstore.PartId]capturedLogicalState
	untrustedTail   *uint64
}

type capturedLogicalState struct {
	sequence   uint64
	activate   bool
	generation journal.GenerationID
}

func NewMigrator(j *journal.Journal, device tapedev.Device, recordSize int, policy SegmentPackingPolicy, previousSegment [16]byte, nextSequence uint64, captured ...map[partstore.PartId]capturedLogicalState) *Migrator {
	if recordSize <= 0 {
		recordSize = defaultRecordSize
	}
	capturedOps := make(map[partstore.PartId]capturedLogicalState)
	if len(captured) > 0 {
		for partID, state := range captured[0] {
			capturedOps[partID] = state
		}
	}
	return &Migrator{
		journal:         j,
		device:          device,
		recordSize:      recordSize,
		policy:          policy,
		previousSegment: previousSegment,
		nextSequence:    nextSequence,
		capturedOps:     capturedOps,
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
	segment   scannedSegment
	Committed bool
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

	candidates, byGen := m.buildCandidates(snap)
	plan := PlanSegment(candidates, m.policy)
	if len(plan.Parts) > 0 && !plan.Ready && !force {
		return MigrationResult{}, nil
	}
	if len(plan.Parts) == 0 && len(candidates) > 0 && !force {
		return MigrationResult{}, nil
	}
	selected := make(map[journal.GenerationID]struct{}, len(plan.Parts))
	for _, candidate := range plan.Parts {
		selected[candidate.Generation] = struct{}{}
	}
	manifestOps := m.pendingLogicalState(snap, selected)
	if len(plan.Parts) == 0 && (len(manifestOps) == 0 || !force) {
		return MigrationResult{}, nil
	}

	if m.untrustedTail != nil {
		if err := sealUntrustedTail(ctx, m.device, *m.untrustedTail); err != nil {
			return MigrationResult{}, fmt.Errorf("sealing failed tape segment at block %d: %w", *m.untrustedTail, err)
		}
		m.untrustedTail = nil
	}
	writer, err := NewSegmentWriter(ctx, m.device, m.recordSize, m.previousSegment, m.nextSequence)
	if err != nil {
		if writer != nil {
			tailBlock := writer.firstBlock
			m.untrustedTail = &tailBlock
		}
		return MigrationResult{}, err
	}
	segmentCommitted := false
	defer func() {
		if !segmentCommitted {
			tailBlock := writer.firstBlock
			m.untrustedTail = &tailBlock
		}
	}()

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
		migrated = append(migrated, MigratedPart{
			PartID:     part.PartID,
			Generation: part.Generation,
			StartBlock: startBlock,
			Length:     part.Length,
			Hash:       part.Hash,
		})
	}
	for _, op := range manifestOps {
		if op.IsActivate() {
			writer.AddActivate(segActivatePayload{
				partID:           op.PartID,
				generation:       op.Generation,
				expectedPrevious: op.ExpectedPrevious,
				sequence:         op.Sequence,
			})
		} else {
			writer.AddDelete(segDeletePayload{
				partID:             op.PartID,
				expectedGeneration: op.ExpectedGeneration,
				sequence:           op.Sequence,
			})
		}
	}

	segmentID, err := writer.Finish(ctx)
	if err != nil {
		// The segment is not committed; recovery ignores it and the parts stay
		// authoritative in the journal.
		return MigrationResult{}, fmt.Errorf("committing tape segment: %w", err)
	}

	segment, err := writer.CommittedSegment()
	if err != nil {
		return MigrationResult{}, err
	}

	m.previousSegment = segmentID
	m.nextSequence = writer.NextSequence()
	for _, op := range manifestOps {
		m.capturedOps[op.PartID] = capturedStateFromOp(op)
	}
	segmentCommitted = true
	return MigrationResult{SegmentID: segmentID, Parts: migrated, segment: segment, Committed: true}, nil
}

// buildCandidates selects live parts whose payload is committed in the journal
// and not yet checkpointed onto tape.
func (m *Migrator) buildCandidates(snap *journal.RecoveryResult) ([]PackCandidate, map[journal.GenerationID]*journal.RecoveredPart) {
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
	return candidates, byGen
}

// pendingLogicalState returns the newest uncaptured state for each part. An
// activation is emitted only when its payload is already on tape or is part of
// this segment; deletions can always be captured. This makes a tape-only
// catalog sufficient to prevent deleted parts from being resurrected.
func (m *Migrator) pendingLogicalState(snap *journal.RecoveryResult, selected map[journal.GenerationID]struct{}) []journal.LogicalOp {
	latest := make(map[partstore.PartId]journal.LogicalOp)
	for _, op := range snap.Ops {
		if current, ok := latest[op.PartID]; !ok || op.Sequence > current.Sequence {
			latest[op.PartID] = op
		}
	}
	ops := make([]journal.LogicalOp, 0, len(latest))
	for partID, op := range latest {
		captured := m.capturedOps[partID]
		if op.Sequence <= captured.sequence {
			continue
		}
		next := capturedStateFromOp(op)
		if captured.sequence > 0 && captured.activate == next.activate && (!next.activate || captured.generation == next.generation) {
			// Journal compaction emits unconditional snapshots with fresh
			// sequence numbers. If tape already represents the same state,
			// advance the watermark without spending another pair of filemarks.
			m.capturedOps[partID] = next
			continue
		}
		if op.IsActivate() {
			if _, inThisSegment := selected[op.Generation]; !inThisSegment {
				part := snap.Parts[op.Generation]
				if part != nil && !part.Checkpointed {
					continue
				}
			}
		}
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, k int) bool { return ops[i].Sequence < ops[k].Sequence })
	return ops
}

func capturedStateFromOp(op journal.LogicalOp) capturedLogicalState {
	return capturedLogicalState{
		sequence:   op.Sequence,
		activate:   op.IsActivate(),
		generation: op.Generation,
	}
}
