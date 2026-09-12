package prometheus

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	pithostesting "github.com/jdillenkofer/pithos/internal/testing"
	client "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

type fakePartStore struct {
	content    []byte
	statsCalls int
	getCalls   int
	listCalls  int
}

func counterValue(t *testing.T, counter client.Counter) float64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, counter.Write(metric))
	return metric.GetCounter().GetValue()
}

func (f *fakePartStore) Start(context.Context) error { return nil }
func (f *fakePartStore) Stop(context.Context) error  { return nil }
func (f *fakePartStore) Capabilities() partstore.Capabilities {
	return partstore.NewCapabilities(partstore.CapabilityTxFreeGetPart)
}
func (f *fakePartStore) PutPart(_ context.Context, _ database.Tx, _ partstore.PartId, reader io.Reader) error {
	var err error
	f.content, err = io.ReadAll(reader)
	return err
}
func (f *fakePartStore) GetPart(context.Context, database.Tx, partstore.PartId) (io.ReadCloser, error) {
	f.getCalls++
	return io.NopCloser(bytes.NewReader(f.content)), nil
}
func (f *fakePartStore) GetPartIds(context.Context, database.Tx) ([]partstore.PartId, error) {
	f.listCalls++
	return nil, nil
}
func (f *fakePartStore) DeletePart(context.Context, database.Tx, partstore.PartId) error { return nil }
func (f *fakePartStore) SupportsStats() bool                                             { return true }
func (f *fakePartStore) Stats(context.Context, database.Tx) (partstore.Stats, error) {
	f.statsCalls++
	return partstore.Stats{Parts: 3, Bytes: 42}, nil
}

func TestMiddlewareTracksOperationsAndForwardsCapabilities(t *testing.T) {
	pithostesting.WithTestRegisterer(t, func(_ client.Registerer) {
		inner := &fakePartStore{}
		middleware := New(inner, nil, "cold", 0).(*PartStoreMiddleware)
		require.True(t, middleware.Capabilities().Has(partstore.CapabilityTxFreeGetPart))

		id, err := partstore.NewRandomPartId()
		require.NoError(t, err)
		require.NoError(t, middleware.PutPart(context.Background(), nil, *id, bytes.NewReader([]byte("data"))))
		reader, err := middleware.GetPart(context.Background(), nil, *id)
		require.NoError(t, err)
		_, err = io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())

		require.Equal(t, float64(1), counterValue(t, metrics.ops.WithLabelValues("cold", "put", "success")))
		require.Equal(t, float64(1), counterValue(t, metrics.ops.WithLabelValues("cold", "get", "success")))
		require.Equal(t, float64(4), counterValue(t, metrics.bytesWritten.WithLabelValues("cold")))
		require.Equal(t, float64(4), counterValue(t, metrics.bytesRead.WithLabelValues("cold")))
	})
}

func TestRefreshUsesCheapStatsProvider(t *testing.T) {
	pithostesting.WithTestRegisterer(t, func(_ client.Registerer) {
		db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "metrics.db"))
		require.NoError(t, err)
		defer db.Close()

		inner := &fakePartStore{}
		middleware := New(inner, db, "archive", 0).(*PartStoreMiddleware)
		require.NoError(t, middleware.refresh(context.Background()))
		require.Equal(t, 1, inner.statsCalls)
		require.Zero(t, inner.getCalls)
		require.Zero(t, inner.listCalls)
		require.Equal(t, float64(3), gaugeValue(t, metrics.parts.WithLabelValues("archive")))
		require.Equal(t, float64(42), gaugeValue(t, metrics.partBytes.WithLabelValues("archive")))
	})
}

func gaugeValue(t *testing.T, gauge client.Gauge) float64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, gauge.Write(metric))
	return metric.GetGauge().GetValue()
}
