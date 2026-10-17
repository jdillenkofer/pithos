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

type seekablePartStore struct {
	fakePartStore
}

type nopReadSeekCloser struct {
	*bytes.Reader
}

func (nopReadSeekCloser) Close() error { return nil }

func (f *seekablePartStore) GetPart(context.Context, database.Tx, partstore.PartId) (io.ReadCloser, error) {
	return nopReadSeekCloser{bytes.NewReader(f.content)}, nil
}

func TestMiddlewarePreservesSeekOnGetPart(t *testing.T) {
	pithostesting.WithTestRegisterer(t, func(_ client.Registerer) {
		id, err := partstore.NewRandomPartId()
		require.NoError(t, err)

		seekable := New(&seekablePartStore{fakePartStore{content: []byte("abcdef")}}, nil, "seekable", 0)
		reader, err := seekable.GetPart(context.Background(), nil, *id)
		require.NoError(t, err)
		seeker, ok := reader.(io.Seeker)
		require.True(t, ok, "reader of a seekable part store must stay seekable")
		_, err = seeker.Seek(4, io.SeekCurrent)
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, "ef", string(data))
		require.Equal(t, float64(2), counterValue(t, metrics.bytesRead.WithLabelValues("seekable")))

		plain := New(&fakePartStore{content: []byte("abcdef")}, nil, "plain", 0)
		reader, err = plain.GetPart(context.Background(), nil, *id)
		require.NoError(t, err)
		_, ok = reader.(io.Seeker)
		require.False(t, ok, "reader of a non-seekable part store must not claim to be seekable")
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
