package tape

import (
	"context"
	"errors"
	"sync"

	tapedev "github.com/jdillenkofer/pithos/internal/tape"
)

type driveJobKind uint8

const (
	driveJobRecall driveJobKind = iota
	driveJobMigration
	maxRecallBurst = 32
)

type driveJobResult struct {
	value any
	err   error
}

type driveJob struct {
	kind        driveJobKind
	targetBlock uint64
	sequence    uint64
	ctx         context.Context
	run         func(context.Context, tapedev.Device) (any, error)
	done        chan driveJobResult
}

// driveScheduler is the sole owner of a tape device after startup recovery.
// Jobs are atomic at tape-motion granularity: no read can reposition the head
// in the middle of a segment write. Recalls outrank migration and are ordered
// by nearest block to reduce aggregate seek distance.
type driveScheduler struct {
	device tapedev.Device

	mu           sync.Mutex
	cond         *sync.Cond
	pending      []*driveJob
	nextSequence uint64
	currentBlock uint64
	recallBurst  int
	closed       bool
	done         chan struct{}
}

func newDriveScheduler(device tapedev.Device) (*driveScheduler, error) {
	pos, err := device.Tell(context.Background())
	if err != nil {
		return nil, err
	}
	s := &driveScheduler{
		device:       device,
		currentBlock: pos.Block,
		done:         make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	go s.loop()
	return s, nil
}

func (s *driveScheduler) recall(ctx context.Context, targetBlock uint64, run func(context.Context, tapedev.Device) (any, error)) (any, error) {
	return s.submit(ctx, driveJobRecall, targetBlock, run)
}

func (s *driveScheduler) migration(ctx context.Context, run func(context.Context, tapedev.Device) (any, error)) (any, error) {
	return s.submit(ctx, driveJobMigration, 0, run)
}

func (s *driveScheduler) submit(ctx context.Context, kind driveJobKind, targetBlock uint64, run func(context.Context, tapedev.Device) (any, error)) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	job := &driveJob{
		kind:        kind,
		targetBlock: targetBlock,
		ctx:         ctx,
		run:         run,
		done:        make(chan driveJobResult, 1),
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, tapedev.ErrClosed
	}
	job.sequence = s.nextSequence
	s.nextSequence++
	s.pending = append(s.pending, job)
	s.cond.Signal()
	s.mu.Unlock()

	select {
	case result := <-job.done:
		return result.value, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *driveScheduler) loop() {
	defer close(s.done)
	for {
		s.mu.Lock()
		for len(s.pending) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			pending := s.pending
			s.pending = nil
			s.mu.Unlock()
			for _, job := range pending {
				job.done <- driveJobResult{err: tapedev.ErrClosed}
			}
			return
		}
		index := s.selectNextLocked()
		job := s.pending[index]
		s.pending = append(s.pending[:index], s.pending[index+1:]...)
		if job.kind == driveJobRecall {
			s.recallBurst++
		} else {
			s.recallBurst = 0
		}
		s.mu.Unlock()

		if err := job.ctx.Err(); err != nil {
			job.done <- driveJobResult{err: err}
			continue
		}
		value, err := job.run(job.ctx, s.device)
		if pos, tellErr := s.device.Tell(context.Background()); tellErr == nil {
			s.mu.Lock()
			s.currentBlock = pos.Block
			s.mu.Unlock()
		}
		job.done <- driveJobResult{value: value, err: err}
	}
}

func (s *driveScheduler) selectNextLocked() int {
	// Reads normally outrank writes, but a sustained recall stream must not
	// starve migration forever or the disk journal will grow without bound.
	if s.recallBurst >= maxRecallBurst {
		bestMigration := -1
		for i, job := range s.pending {
			if job.ctx.Err() != nil || job.kind != driveJobMigration {
				continue
			}
			if bestMigration == -1 || job.sequence < s.pending[bestMigration].sequence {
				bestMigration = i
			}
		}
		if bestMigration >= 0 {
			return bestMigration
		}
	}
	best := -1
	var bestDistance uint64
	for i, job := range s.pending {
		if job.ctx.Err() != nil {
			continue
		}
		if job.kind != driveJobRecall {
			continue
		}
		distance := absBlockDistance(s.currentBlock, job.targetBlock)
		if best == -1 || distance < bestDistance || distance == bestDistance && job.sequence < s.pending[best].sequence {
			best = i
			bestDistance = distance
		}
	}
	if best >= 0 {
		return best
	}
	// No live recall is queued. Preserve FIFO order for migrations.
	best = 0
	for i := 1; i < len(s.pending); i++ {
		if s.pending[i].sequence < s.pending[best].sequence {
			best = i
		}
	}
	return best
}

func absBlockDistance(a, b uint64) uint64 {
	if a >= b {
		return a - b
	}
	return b - a
}

func (s *driveScheduler) status() (queueDepth int, currentBlock uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending), s.currentBlock
}

func (s *driveScheduler) stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.cond.Broadcast()
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return errors.Join(errors.New("stopping tape drive scheduler"), ctx.Err())
	}
}
