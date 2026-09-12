package authentication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// SnapshotCoordinator loads an initial snapshot synchronously, then refreshes
// it in the background. Readers always receive the most recently valid
// immutable snapshot and never invoke the loader themselves.
type SnapshotCoordinator[T any] struct {
	load      func(context.Context) (*T, error)
	equal     func(*T, *T) bool
	onChanged func(*T)
	provider  string

	snapshot atomic.Pointer[T]

	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

func NewSnapshotCoordinator[T any](ctx context.Context, provider string, interval time.Duration, load func(context.Context) (*T, error), equal func(*T, *T) bool, onChanged func(*T)) (*SnapshotCoordinator[T], error) {
	if interval < 0 {
		return nil, fmt.Errorf("credentials reload interval must not be negative")
	}

	initial, err := load(ctx)
	if err != nil {
		return nil, err
	}

	coordinator := &SnapshotCoordinator[T]{
		load:      load,
		equal:     equal,
		onChanged: onChanged,
		provider:  provider,
		done:      make(chan struct{}),
	}
	coordinator.snapshot.Store(initial)
	observeCredentialSnapshotAge(provider, 0)

	if interval == 0 {
		close(coordinator.done)
		return coordinator, nil
	}

	refreshCtx, cancel := context.WithCancel(context.Background())
	coordinator.cancel = cancel
	go coordinator.refreshLoop(refreshCtx, interval)
	return coordinator, nil
}

func (c *SnapshotCoordinator[T]) refreshLoop(ctx context.Context, interval time.Duration) {
	defer close(c.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastSuccess := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			next, err := c.load(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				observeCredentialSnapshotReload(c.provider, false, time.Since(lastSuccess).Seconds())
				slog.Error("Failed to reload credential snapshot; retaining last-known-good credentials", "provider", c.provider, "error", err)
				continue
			}
			current := c.snapshot.Load()
			if c.equal == nil || !c.equal(current, next) {
				c.snapshot.Store(next)
				if c.onChanged != nil {
					c.onChanged(next)
				}
			}
			lastSuccess = time.Now()
			observeCredentialSnapshotReload(c.provider, true, 0)
		}
	}
}

func (c *SnapshotCoordinator[T]) Snapshot() *T {
	return c.snapshot.Load()
}

// Close stops background refresh and waits for an in-progress refresh to end.
func (c *SnapshotCoordinator[T]) Close() error {
	c.closeOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		<-c.done
	})
	return nil
}
