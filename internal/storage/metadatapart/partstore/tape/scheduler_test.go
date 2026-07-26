package tape

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	tapedev "github.com/jdillenkofer/pithos/internal/tape"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestDriveSchedulerSelectsNearestRecallBeforeMigration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	s := &driveScheduler{currentBlock: 100}
	migration := &driveJob{kind: driveJobMigration, sequence: 0, ctx: ctx}
	far := &driveJob{kind: driveJobRecall, targetBlock: 180, sequence: 1, ctx: ctx}
	near := &driveJob{kind: driveJobRecall, targetBlock: 95, sequence: 2, ctx: ctx}
	s.pending = []*driveJob{migration, far, near}
	require.Equal(t, 2, s.selectNextLocked())
}

func TestDriveSchedulerBoundsRecallStarvation(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	s := &driveScheduler{currentBlock: 100, recallBurst: maxRecallBurst}
	recall := &driveJob{kind: driveJobRecall, targetBlock: 100, sequence: 0, ctx: ctx}
	migration := &driveJob{kind: driveJobMigration, sequence: 1, ctx: ctx}
	s.pending = []*driveJob{recall, migration}
	require.Equal(t, 1, s.selectNextLocked())
}

func TestDriveSchedulerRunsWholeJobsExclusively(t *testing.T) {
	testutils.SkipIfIntegration(t)

	dev := openSimulator(t)
	scheduler, err := newDriveScheduler(dev)
	require.NoError(t, err)
	defer scheduler.stop(context.Background())

	const jobs = 24
	var active atomic.Int64
	var maximum atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for index := range jobs {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			run := func(context.Context, tapedev.Device) (any, error) {
				now := active.Add(1)
				for {
					old := maximum.Load()
					if now <= old || maximum.CompareAndSwap(old, now) {
						break
					}
				}
				active.Add(-1)
				return nil, nil
			}
			var jobErr error
			if index%2 == 0 {
				_, jobErr = scheduler.recall(context.Background(), uint64(index), run)
			} else {
				_, jobErr = scheduler.migration(context.Background(), run)
			}
			require.NoError(t, jobErr)
		}(index)
	}
	close(start)
	wg.Wait()
	require.Equal(t, int64(1), maximum.Load())
}
