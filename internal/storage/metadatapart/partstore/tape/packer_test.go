package tape

import (
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func ptr[T any](v T) *T { return &v }

func packCandidate(t *testing.T, obj *partstore.ObjectId, partNum, partCount *uint64, length uint64, arrival uint64, age time.Duration) PackCandidate {
	t.Helper()
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	gen, err := journal.NewGenerationID()
	require.NoError(t, err)
	return PackCandidate{
		PartID:     *id,
		Generation: gen,
		Length:     length,
		ObjectID:   obj,
		PartNumber: partNum,
		PartCount:  partCount,
		Arrival:    arrival,
		Age:        age,
	}
}

func partNumbers(plan SegmentPlan) []uint64 {
	var nums []uint64
	for _, p := range plan.Parts {
		if p.PartNumber != nil {
			nums = append(nums, *p.PartNumber)
		}
	}
	return nums
}

func TestPlanCompleteObjectInPartOrder(t *testing.T) {
	testutils.SkipIfIntegration(t)

	obj := partstore.DeriveObjectId("b", "k", "u")
	// Arrival order is scrambled; part-number order must win.
	cands := []PackCandidate{
		packCandidate(t, &obj, ptr(uint64(3)), ptr(uint64(3)), 100, 30, 0),
		packCandidate(t, &obj, ptr(uint64(1)), ptr(uint64(3)), 100, 10, 0),
		packCandidate(t, &obj, ptr(uint64(2)), ptr(uint64(3)), 100, 20, 0),
	}
	plan := PlanSegment(cands, DefaultPackingPolicy())
	require.Equal(t, []uint64{1, 2, 3}, partNumbers(plan))
	require.Equal(t, int64(300), plan.Bytes)
}

func TestPlanDefersYoungIncompleteObject(t *testing.T) {
	testutils.SkipIfIntegration(t)

	obj := partstore.DeriveObjectId("b", "k", "u")
	cands := []PackCandidate{
		packCandidate(t, &obj, ptr(uint64(1)), ptr(uint64(3)), 100, 10, time.Second),
		packCandidate(t, &obj, ptr(uint64(2)), ptr(uint64(3)), 100, 20, time.Second),
	}
	policy := DefaultPackingPolicy()
	plan := PlanSegment(cands, policy)
	require.Empty(t, plan.Parts, "young incomplete object should be deferred")
	require.False(t, plan.Ready)
}

func TestPlanPacksAgedIncompleteRun(t *testing.T) {
	testutils.SkipIfIntegration(t)

	obj := partstore.DeriveObjectId("b", "k", "u")
	// Parts 1,2 present and aged; part 3 missing. Pack the available run.
	cands := []PackCandidate{
		packCandidate(t, &obj, ptr(uint64(1)), ptr(uint64(5)), 100, 10, time.Minute),
		packCandidate(t, &obj, ptr(uint64(2)), ptr(uint64(5)), 100, 20, time.Minute),
		packCandidate(t, &obj, ptr(uint64(4)), ptr(uint64(5)), 100, 40, time.Minute), // gap at 3
	}
	plan := PlanSegment(cands, DefaultPackingPolicy())
	require.Equal(t, []uint64{1, 2}, partNumbers(plan), "run stops at the gap")
	require.True(t, plan.Ready, "aged parts force readiness")
}

func TestPlanEagerWhenNotPreferringFullObject(t *testing.T) {
	testutils.SkipIfIntegration(t)

	obj := partstore.DeriveObjectId("b", "k", "u")
	cands := []PackCandidate{
		packCandidate(t, &obj, ptr(uint64(1)), ptr(uint64(3)), 100, 10, time.Second),
		packCandidate(t, &obj, ptr(uint64(2)), ptr(uint64(3)), 100, 20, time.Second),
	}
	policy := DefaultPackingPolicy()
	policy.PreferFullObject = false
	plan := PlanSegment(cands, policy)
	require.Equal(t, []uint64{1, 2}, partNumbers(plan))
}

func TestPlanUngroupedInArrivalOrder(t *testing.T) {
	testutils.SkipIfIntegration(t)

	cands := []PackCandidate{
		packCandidate(t, nil, nil, nil, 10, 30, 0),
		packCandidate(t, nil, nil, nil, 10, 10, 0),
		packCandidate(t, nil, nil, nil, 10, 20, 0),
	}
	plan := PlanSegment(cands, DefaultPackingPolicy())
	require.Len(t, plan.Parts, 3)
	require.Equal(t, uint64(10), plan.Parts[0].Arrival)
	require.Equal(t, uint64(20), plan.Parts[1].Arrival)
	require.Equal(t, uint64(30), plan.Parts[2].Arrival)
}

func TestPlanCompleteObjectsBeforeUngrouped(t *testing.T) {
	testutils.SkipIfIntegration(t)

	obj := partstore.DeriveObjectId("b", "k", "u")
	cands := []PackCandidate{
		packCandidate(t, nil, nil, nil, 10, 5, 0), // ungrouped, earliest arrival
		packCandidate(t, &obj, ptr(uint64(1)), ptr(uint64(1)), 10, 50, 0),
	}
	plan := PlanSegment(cands, DefaultPackingPolicy())
	require.Len(t, plan.Parts, 2)
	// Complete object is prioritized ahead of the earlier ungrouped part.
	require.NotNil(t, plan.Parts[0].PartNumber)
	require.Nil(t, plan.Parts[1].PartNumber)
}

func TestPlanRespectsMaxBytes(t *testing.T) {
	testutils.SkipIfIntegration(t)

	policy := DefaultPackingPolicy()
	policy.MaxBytes = 250
	policy.TargetBytes = 1000
	cands := []PackCandidate{
		packCandidate(t, nil, nil, nil, 100, 10, 0),
		packCandidate(t, nil, nil, nil, 100, 20, 0),
		packCandidate(t, nil, nil, nil, 100, 30, 0), // would exceed 250
	}
	plan := PlanSegment(cands, policy)
	require.Len(t, plan.Parts, 2)
	require.Equal(t, int64(200), plan.Bytes)
}

func TestPlanOversizedSinglePartMakesProgress(t *testing.T) {
	testutils.SkipIfIntegration(t)

	policy := DefaultPackingPolicy()
	policy.MaxBytes = 100
	cands := []PackCandidate{
		packCandidate(t, nil, nil, nil, 500, 10, 0), // alone exceeds MaxBytes
		packCandidate(t, nil, nil, nil, 10, 20, 0),
	}
	plan := PlanSegment(cands, policy)
	require.Len(t, plan.Parts, 1)
	require.Equal(t, int64(500), plan.Bytes)
}

func TestPlanReadyWhenTargetReached(t *testing.T) {
	testutils.SkipIfIntegration(t)

	policy := DefaultPackingPolicy()
	policy.TargetBytes = 150
	cands := []PackCandidate{
		packCandidate(t, nil, nil, nil, 100, 10, 0),
		packCandidate(t, nil, nil, nil, 100, 20, 0),
	}
	plan := PlanSegment(cands, policy)
	require.True(t, plan.Ready)
}

func TestPlanForcesOldestBeyondMaxOpenObjects(t *testing.T) {
	testutils.SkipIfIntegration(t)

	policy := DefaultPackingPolicy()
	policy.MaxOpenObjects = 1
	objA := partstore.DeriveObjectId("b", "k", "a")
	objB := partstore.DeriveObjectId("b", "k", "bee")
	cands := []PackCandidate{
		packCandidate(t, &objA, ptr(uint64(1)), ptr(uint64(2)), 100, 10, time.Second),
		packCandidate(t, &objB, ptr(uint64(1)), ptr(uint64(2)), 100, 20, time.Second),
	}
	// Two open incomplete objects exceed MaxOpenObjects=1, so they are packed
	// rather than deferred indefinitely.
	plan := PlanSegment(cands, policy)
	require.NotEmpty(t, plan.Parts)
}
