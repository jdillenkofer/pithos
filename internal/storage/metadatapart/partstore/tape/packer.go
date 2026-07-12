package tape

import (
	"sort"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
)

// SegmentPackingPolicy tunes how staged parts are grouped into tape segments.
type SegmentPackingPolicy struct {
	// TargetBytes is the size a segment aims for before it is flushed.
	TargetBytes int64
	// MaxBytes caps a segment; adding a part that would exceed it defers the
	// part to the next segment (unless the segment is empty and the part alone
	// is larger).
	MaxBytes int64
	// MaxWait is how long an incomplete object may wait for its missing parts
	// before its available ordered run is packed anyway.
	MaxWait time.Duration
	// PreferFullObject keeps incomplete objects waiting (up to MaxWait) so their
	// parts pack together. When false, parts pack in arrival order regardless.
	PreferFullObject bool
	// MaxOpenObjects bounds how many incomplete objects may be deferred; beyond
	// it, the oldest are packed even if still incomplete.
	MaxOpenObjects int
}

// DefaultPackingPolicy returns the recommended starting policy.
func DefaultPackingPolicy() SegmentPackingPolicy {
	return SegmentPackingPolicy{
		TargetBytes:      1 << 30, // 1 GiB
		MaxBytes:         4 << 30, // 4 GiB
		MaxWait:          30 * time.Second,
		PreferFullObject: true,
		MaxOpenObjects:   10_000,
	}
}

// PackCandidate is a committed, activated journal part eligible for migration.
type PackCandidate struct {
	PartID     partstore.PartId
	Generation journal.GenerationID
	Length     uint64
	ObjectID   *partstore.ObjectId
	PartNumber *uint64
	PartCount  *uint64
	// Arrival orders parts by when they were staged (e.g. activation sequence).
	Arrival uint64
	// Age is how long the part has been staged.
	Age time.Duration
}

// SegmentPlan is an ordered set of parts to write into the next segment.
type SegmentPlan struct {
	Parts []PackCandidate
	Bytes int64
	// Ready reports whether the segment should be flushed now (target reached
	// or a part has waited past MaxWait).
	Ready bool
}

// PlanSegment orders candidates into the next segment per the policy. It is a
// pure function: it neither reads nor mutates any device or journal state.
//
// Ordering priority:
//  1. complete objects (all parts present), oldest first, in part-number order;
//  2. incomplete objects past MaxWait (or forced by MaxOpenObjects), packing
//     their available consecutive run, oldest first;
//  3. ungrouped parts in arrival order.
//
// Young incomplete objects are deferred so their parts pack together, unless
// PreferFullObject is false.
func PlanSegment(candidates []PackCandidate, policy SegmentPackingPolicy) SegmentPlan {
	grouped, ungrouped := groupCandidates(candidates)

	type objectGroup struct {
		parts      []PackCandidate
		minArrival uint64
		complete   bool
		aged       bool
	}
	groups := make([]objectGroup, 0, len(grouped))
	for _, parts := range grouped {
		sortByPartNumberThenArrival(parts)
		g := objectGroup{parts: parts, minArrival: minArrival(parts), complete: objectComplete(parts)}
		for _, p := range parts {
			if p.Age >= policy.MaxWait {
				g.aged = true
			}
		}
		groups = append(groups, g)
	}
	sort.Slice(groups, func(a, b int) bool { return groups[a].minArrival < groups[b].minArrival })

	// When PreferFullObject is off, or too many objects are open, incomplete
	// objects become eligible too.
	openCount := 0
	for i := range groups {
		if !groups[i].complete && !groups[i].aged {
			openCount++
		}
	}
	forceOldest := policy.MaxOpenObjects > 0 && openCount > policy.MaxOpenObjects

	var ordered []PackCandidate
	anyAged := false
	// Priority 1: complete objects.
	for _, g := range groups {
		if g.complete {
			ordered = append(ordered, g.parts...)
			if g.aged {
				anyAged = true
			}
		}
	}
	// Priority 2: eligible incomplete objects (aged, forced, or eager).
	for _, g := range groups {
		if g.complete {
			continue
		}
		eligible := g.aged || forceOldest || !policy.PreferFullObject
		if !eligible {
			continue
		}
		if g.aged {
			anyAged = true
		}
		ordered = append(ordered, availableRun(g.parts)...)
	}
	// Priority 3: ungrouped parts in arrival order.
	sort.Slice(ungrouped, func(a, b int) bool { return ungrouped[a].Arrival < ungrouped[b].Arrival })
	for _, p := range ungrouped {
		if p.Age >= policy.MaxWait {
			anyAged = true
		}
	}
	ordered = append(ordered, ungrouped...)

	plan := fillSegment(ordered, policy)
	if plan.Bytes >= policy.TargetBytes || anyAged {
		plan.Ready = true
	}
	return plan
}

// fillSegment greedily adds parts until MaxBytes would be exceeded. An empty
// segment always takes the first part even if it alone exceeds MaxBytes, so a
// single oversized part still makes progress.
func fillSegment(ordered []PackCandidate, policy SegmentPackingPolicy) SegmentPlan {
	var plan SegmentPlan
	for _, p := range ordered {
		next := plan.Bytes + int64(p.Length)
		if policy.MaxBytes > 0 && next > policy.MaxBytes && len(plan.Parts) > 0 {
			break
		}
		plan.Parts = append(plan.Parts, p)
		plan.Bytes = next
	}
	return plan
}

func groupCandidates(candidates []PackCandidate) (map[partstore.ObjectId][]PackCandidate, []PackCandidate) {
	grouped := map[partstore.ObjectId][]PackCandidate{}
	var ungrouped []PackCandidate
	for _, c := range candidates {
		if c.ObjectID == nil {
			ungrouped = append(ungrouped, c)
			continue
		}
		grouped[*c.ObjectID] = append(grouped[*c.ObjectID], c)
	}
	return grouped, ungrouped
}

func sortByPartNumberThenArrival(parts []PackCandidate) {
	sort.Slice(parts, func(a, b int) bool {
		pa, pb := parts[a].PartNumber, parts[b].PartNumber
		switch {
		case pa != nil && pb != nil && *pa != *pb:
			return *pa < *pb
		case pa != nil && pb == nil:
			return true
		case pa == nil && pb != nil:
			return false
		default:
			return parts[a].Arrival < parts[b].Arrival
		}
	})
}

func minArrival(parts []PackCandidate) uint64 {
	m := parts[0].Arrival
	for _, p := range parts[1:] {
		if p.Arrival < m {
			m = p.Arrival
		}
	}
	return m
}

// objectComplete reports whether the group holds every part of a known-size
// object with consecutive part numbers 1..PartCount.
func objectComplete(parts []PackCandidate) bool {
	var partCount *uint64
	seen := map[uint64]bool{}
	for _, p := range parts {
		if p.PartCount != nil {
			partCount = p.PartCount
		}
		if p.PartNumber == nil {
			return false
		}
		seen[*p.PartNumber] = true
	}
	if partCount == nil || *partCount == 0 {
		return false
	}
	if uint64(len(seen)) != *partCount {
		return false
	}
	for n := uint64(1); n <= *partCount; n++ {
		if !seen[n] {
			return false
		}
	}
	return true
}

// availableRun returns the consecutive prefix of an incomplete object's parts
// starting from its lowest part number, stopping at the first gap. Parts
// without numbers keep their sorted order and are all included.
func availableRun(parts []PackCandidate) []PackCandidate {
	if len(parts) == 0 {
		return nil
	}
	if parts[0].PartNumber == nil {
		return parts
	}
	run := []PackCandidate{parts[0]}
	prev := *parts[0].PartNumber
	for _, p := range parts[1:] {
		if p.PartNumber == nil {
			break
		}
		if *p.PartNumber != prev+1 {
			break
		}
		run = append(run, p)
		prev = *p.PartNumber
	}
	return run
}
