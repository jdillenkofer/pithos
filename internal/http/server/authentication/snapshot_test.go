package authentication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pithostesting "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSnapshot struct{ value string }

func TestSnapshotCoordinatorRequiresInitialSnapshot(t *testing.T) {
	_, err := NewSnapshotCoordinator(context.Background(), "test", time.Second,
		func(context.Context) (*testSnapshot, error) { return nil, errors.New("unavailable") }, nil, nil)
	require.EqualError(t, err, "unavailable")
}

func TestSnapshotCoordinatorRefreshesRetainsAndCloses(t *testing.T) {
	pithostesting.WithTestRegisterer(t, func(_ prometheus.Registerer) {
		var value atomic.Pointer[testSnapshot]
		value.Store(&testSnapshot{value: "old"})
		var fail atomic.Bool
		var loads atomic.Int32
		coordinator, err := NewSnapshotCoordinator(context.Background(), "test", 5*time.Millisecond,
			func(ctx context.Context) (*testSnapshot, error) {
				loads.Add(1)
				if fail.Load() {
					return nil, errors.New("unavailable")
				}
				return value.Load(), nil
			},
			func(current, next *testSnapshot) bool { return current.value == next.value }, nil,
		)
		require.NoError(t, err)

		value.Store(&testSnapshot{value: "new"})
		require.Eventually(t, func() bool { return coordinator.Snapshot().value == "new" }, time.Second, time.Millisecond)

		fail.Store(true)
		require.Eventually(t, func() bool {
			metric := &dto.Metric{}
			require.NoError(t, credentialSnapshotReloadFailures.WithLabelValues("test").Write(metric))
			return metric.GetCounter().GetValue() > 0
		}, time.Second, time.Millisecond)
		assert.Equal(t, "new", coordinator.Snapshot().value)

		metric := &dto.Metric{}
		require.NoError(t, credentialSnapshotReloadSuccesses.WithLabelValues("test").Write(metric))
		assert.Greater(t, metric.GetCounter().GetValue(), float64(0))

		require.NoError(t, coordinator.Close())
		loadsAfterClose := loads.Load()
		time.Sleep(20 * time.Millisecond)
		assert.Equal(t, loadsAfterClose, loads.Load())
		require.NoError(t, coordinator.Close())
	})
}

func TestSnapshotAgeCollectorCalculatesAgeWhenCollected(t *testing.T) {
	collector := newSnapshotAgeCollector()
	collector.lastSuccess["file"] = time.Now().Add(-time.Minute)
	metrics := make(chan prometheus.Metric, 1)
	collector.Collect(metrics)
	close(metrics)

	metric := &dto.Metric{}
	require.NoError(t, (<-metrics).Write(metric))
	assert.InDelta(t, time.Minute.Seconds(), metric.GetGauge().GetValue(), 0.1)
}

func TestSnapshotCoordinatorZeroIntervalDoesNotRefresh(t *testing.T) {
	pithostesting.WithTestRegisterer(t, func(_ prometheus.Registerer) {
		var loads atomic.Int32
		coordinator, err := NewSnapshotCoordinator(context.Background(), "test-zero", 0,
			func(context.Context) (*testSnapshot, error) {
				loads.Add(1)
				return &testSnapshot{value: "initial"}, nil
			}, nil, nil)
		require.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		assert.Equal(t, int32(1), loads.Load())
		require.NoError(t, coordinator.Close())
	})
}
