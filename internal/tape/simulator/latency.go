package simulator

import (
	"context"
	"time"
)

// LatencyProfile describes simulated tape drive timing. The zero value
// disables all simulated latency.
//
// Positioning costs are estimated from the physical distance the head has to
// travel, using the byte offset within the tape file as a proxy for the
// linear position on the tape: a repositioning over d bytes on a tape with
// capacity c costs MinSeek + FullTapeLocate*(d/c) (respectively
// FullTapeRewind for rewinds). Repositioning to the current position is
// free, so sequential access stays streaming. Transfers cost
// bytes/throughput.
type LatencyProfile struct {
	// LoadTime is charged when the device is opened (cartridge load and
	// thread time).
	LoadTime time.Duration
	// FullTapeRewind is the rewind time from the physical end of the tape;
	// actual rewinds are scaled by the fraction of tape traversed.
	FullTapeRewind time.Duration
	// FullTapeLocate is the high-speed locate time across the entire tape;
	// actual locates are scaled by the fraction of tape traversed.
	FullTapeLocate time.Duration
	// MinSeek is the start/stop and settle floor charged for any
	// repositioning, however small.
	MinSeek time.Duration
	// ReadThroughput is the sustained read rate in bytes per second.
	ReadThroughput int64
	// WriteThroughput is the sustained write rate in bytes per second.
	WriteThroughput int64
	// FilemarkWriteTime is charged per written filemark (a filemark forces
	// a buffer flush on real drives).
	FilemarkWriteTime time.Duration
}

// DefaultLTO8Profile returns timing in the class of an LTO-8 full-height
// drive: ~360 MB/s native throughput, average locate from BOT around a
// minute, full rewind about one and a half minutes.
func DefaultLTO8Profile() LatencyProfile {
	return LatencyProfile{
		LoadTime:          15 * time.Second,
		FullTapeRewind:    90 * time.Second,
		FullTapeLocate:    110 * time.Second,
		MinSeek:           2 * time.Second,
		ReadThroughput:    360 << 20,
		WriteThroughput:   360 << 20,
		FilemarkWriteTime: 1 * time.Second,
	}
}

// defaultScaleCapacity is the assumed tape length for distance scaling when
// the tape file has unlimited capacity (LTO-8 native capacity, 12 TB).
const defaultScaleCapacity = 12_000_000_000_000

// seekCost estimates a locate between two byte offsets. Staying in place is
// free: sequential continuation does not interrupt streaming.
func (p LatencyProfile) seekCost(from, to, scaleCapacity int64) time.Duration {
	if from == to {
		return 0
	}
	dist := from - to
	if dist < 0 {
		dist = -dist
	}
	return p.MinSeek + scaleByDistance(p.FullTapeLocate, dist, scaleCapacity)
}

// rewindCost estimates a rewind from the given distance to the beginning of
// the tape.
func (p LatencyProfile) rewindCost(distanceFromBOT, scaleCapacity int64) time.Duration {
	if distanceFromBOT <= 0 {
		return 0
	}
	return p.MinSeek + scaleByDistance(p.FullTapeRewind, distanceFromBOT, scaleCapacity)
}

func (p LatencyProfile) transferCost(bytes int64, throughput int64) time.Duration {
	if bytes <= 0 || throughput <= 0 {
		return 0
	}
	return time.Duration(float64(bytes) / float64(throughput) * float64(time.Second))
}

func (p LatencyProfile) filemarkCost(count int) time.Duration {
	return time.Duration(count) * p.FilemarkWriteTime
}

func scaleByDistance(full time.Duration, dist, scaleCapacity int64) time.Duration {
	if scaleCapacity <= 0 {
		return 0
	}
	fraction := float64(dist) / float64(scaleCapacity)
	if fraction > 1 {
		fraction = 1
	}
	return time.Duration(fraction * float64(full))
}

// sleepFunc waits for the given duration; tests inject a recorder instead.
type sleepFunc func(ctx context.Context, d time.Duration) error

func contextSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
