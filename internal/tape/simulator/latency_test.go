package simulator

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

// sleepRecorder captures charged latencies instead of sleeping.
type sleepRecorder struct {
	sleeps []time.Duration
}

func (r *sleepRecorder) sleep(_ context.Context, d time.Duration) error {
	if d > 0 {
		r.sleeps = append(r.sleeps, d)
	}
	return nil
}

func openRecordedDevice(t *testing.T, opts Options) (*Device, *sleepRecorder) {
	t.Helper()
	recorder := &sleepRecorder{}
	dev, err := open(context.Background(), filepath.Join(t.TempDir(), "tape.sim"), opts, recorder.sleep)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dev.Close())
	})
	return dev, recorder
}

func TestZeroProfileNeverSleeps(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	dev, recorder := openRecordedDevice(t, Options{})

	require.NoError(t, dev.WriteRecord(ctx, make([]byte, 1024)))
	require.NoError(t, dev.WriteFilemarks(ctx, 1))
	require.NoError(t, dev.Rewind(ctx))
	_, err := dev.ReadRecord(ctx, make([]byte, 2048))
	require.NoError(t, err)
	require.NoError(t, dev.SeekToEOD(ctx))

	require.Empty(t, recorder.sleeps)
}

func TestLoadTimeIsChargedOnOpen(t *testing.T) {
	testutils.SkipIfIntegration(t)
	_, recorder := openRecordedDevice(t, Options{
		Latency: LatencyProfile{LoadTime: 15 * time.Second},
	})
	require.Equal(t, []time.Duration{15 * time.Second}, recorder.sleeps)
}

func TestTransferCostFollowsThroughput(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	dev, recorder := openRecordedDevice(t, Options{
		Latency: LatencyProfile{
			ReadThroughput:  1 << 20, // 1 MiB/s
			WriteThroughput: 2 << 20, // 2 MiB/s
		},
	})

	require.NoError(t, dev.WriteRecord(ctx, make([]byte, 1<<20)))
	require.Equal(t, []time.Duration{500 * time.Millisecond}, recorder.sleeps)

	recorder.sleeps = nil
	require.NoError(t, dev.Rewind(ctx)) // free: MinSeek is zero in this profile
	_, err := dev.ReadRecord(ctx, make([]byte, 1<<20))
	require.NoError(t, err)
	require.Equal(t, []time.Duration{time.Second}, recorder.sleeps)
}

func TestSeekCostScalesWithDistance(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	minSeek := 2 * time.Second
	dev, recorder := openRecordedDevice(t, Options{
		Capacity: 1 << 20,
		Latency: LatencyProfile{
			FullTapeLocate: 100 * time.Second,
			MinSeek:        minSeek,
		},
	})

	for range 8 {
		require.NoError(t, dev.WriteRecord(ctx, make([]byte, 64<<10)))
	}

	// Locating to the current position is free: sequential access streams.
	recorder.sleeps = nil
	require.NoError(t, dev.LocateBlock(ctx, 8))
	require.Empty(t, recorder.sleeps)

	// A short hop costs at least MinSeek; a long one costs more.
	require.NoError(t, dev.LocateBlock(ctx, 7))
	require.Len(t, recorder.sleeps, 1)
	shortHop := recorder.sleeps[0]
	require.GreaterOrEqual(t, shortHop, minSeek)

	recorder.sleeps = nil
	require.NoError(t, dev.LocateBlock(ctx, 0))
	require.Len(t, recorder.sleeps, 1)
	longHop := recorder.sleeps[0]
	require.Greater(t, longHop, shortHop)
}

func TestRewindCostScalesWithHeadPosition(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	dev, recorder := openRecordedDevice(t, Options{
		Capacity: 1 << 20,
		Latency: LatencyProfile{
			FullTapeRewind: 90 * time.Second,
			MinSeek:        time.Second,
		},
	})

	// Rewinding at the beginning of the tape is free.
	require.NoError(t, dev.Rewind(ctx))
	require.Empty(t, recorder.sleeps)

	require.NoError(t, dev.WriteRecord(ctx, make([]byte, 256<<10)))
	require.NoError(t, dev.Rewind(ctx))
	require.Len(t, recorder.sleeps, 1)
	nearRewind := recorder.sleeps[0]

	require.NoError(t, dev.SeekToEOD(ctx))
	require.NoError(t, dev.WriteRecord(ctx, make([]byte, 512<<10)))
	recorder.sleeps = nil
	require.NoError(t, dev.Rewind(ctx))
	require.Len(t, recorder.sleeps, 1)
	require.Greater(t, recorder.sleeps[0], nearRewind)
}

func TestFilemarkWriteCost(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	dev, recorder := openRecordedDevice(t, Options{
		Latency: LatencyProfile{FilemarkWriteTime: time.Second},
	})
	require.NoError(t, dev.WriteFilemarks(ctx, 3))
	require.Equal(t, []time.Duration{3 * time.Second}, recorder.sleeps)
}

func TestLatencySleepIsContextCancellable(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := contextSleep(ctx, time.Hour)
	require.ErrorIs(t, err, context.Canceled)

	// An operation interrupted mid-sleep surfaces the context error.
	_, err = Open(ctx, filepath.Join(t.TempDir(), "tape.sim"), Options{
		Latency: LatencyProfile{LoadTime: time.Hour},
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestLatencyCostMath(t *testing.T) {
	testutils.SkipIfIntegration(t)
	p := LatencyProfile{
		FullTapeRewind: 90 * time.Second,
		FullTapeLocate: 100 * time.Second,
		MinSeek:        2 * time.Second,
	}

	require.Equal(t, time.Duration(0), p.seekCost(500, 500, 1000))
	require.Equal(t, 2*time.Second+50*time.Second, p.seekCost(0, 500, 1000))
	require.Equal(t, 2*time.Second+50*time.Second, p.seekCost(500, 0, 1000))
	// Distances are clamped to a full tape pass.
	require.Equal(t, 2*time.Second+100*time.Second, p.seekCost(0, 5000, 1000))

	require.Equal(t, time.Duration(0), p.rewindCost(0, 1000))
	require.Equal(t, 2*time.Second+45*time.Second, p.rewindCost(500, 1000))

	require.Equal(t, time.Second, p.transferCost(1<<20, 1<<20))
	require.Equal(t, time.Duration(0), p.transferCost(0, 1<<20))
	require.Equal(t, time.Duration(0), p.transferCost(1<<20, 0))
}
