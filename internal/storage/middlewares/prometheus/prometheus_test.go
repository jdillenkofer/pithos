package prometheus

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	_ "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, counter.Write(metric))
	return metric.GetCounter().GetValue()
}

func TestOperationOutcome(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "success", want: "success"},
		{name: "missing bucket", err: storage.ErrNoSuchBucket, want: "not_found"},
		{name: "missing object", err: storage.ErrNoSuchKey, want: "not_found"},
		{name: "missing website configuration", err: storage.ErrNoSuchWebsiteConfiguration, want: "not_found"},
		{name: "missing CORS configuration", err: storage.ErrNoSuchCORSConfiguration, want: "not_found"},
		{name: "missing lifecycle configuration", err: storage.ErrNoSuchLifecycleConfiguration, want: "not_found"},
		{name: "wrapped expected error", err: fmt.Errorf("load lifecycle: %w", storage.ErrNoSuchLifecycleConfiguration), want: "not_found"},
		{name: "precondition failed", err: storage.ErrPreconditionFailed, want: "rejected"},
		{name: "not modified", err: storage.ErrNotModified, want: "rejected"},
		{name: "backend failure", err: errors.New("database unavailable"), want: "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, operationOutcome(tt.err))
		})
	}
}

func TestObserveOperationKeepsExpectedOutcomesOutOfFailedCounter(t *testing.T) {
	middleware := &prometheusStorageMiddleware{
		failedApiOpsCounter:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "failed"}, []string{"type"}),
		successfulApiOpsCounter: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "successful"}, []string{"type"}),
		apiOpsCounter:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "operations"}, []string{"type", "outcome"}),
	}

	middleware.observeOperation("GetBucketLifecycle", storage.ErrNoSuchLifecycleConfiguration)
	middleware.observeOperation("GetBucketLifecycle", errors.New("database unavailable"))
	middleware.observeOperation("GetBucketLifecycle", nil)

	require.Equal(t, float64(1), counterValue(t, middleware.apiOpsCounter.WithLabelValues("GetBucketLifecycle", "not_found")))
	require.Equal(t, float64(1), counterValue(t, middleware.apiOpsCounter.WithLabelValues("GetBucketLifecycle", "error")))
	require.Equal(t, float64(1), counterValue(t, middleware.apiOpsCounter.WithLabelValues("GetBucketLifecycle", "success")))
	require.Equal(t, float64(1), counterValue(t, middleware.failedApiOpsCounter.WithLabelValues("GetBucketLifecycle")))
	require.Equal(t, float64(1), counterValue(t, middleware.successfulApiOpsCounter.WithLabelValues("GetBucketLifecycle")))
}
