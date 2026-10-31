package gc

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

type failingTxFreeDeleteStore struct {
	partstore.PartStore
}

func (s *failingTxFreeDeleteStore) Capabilities() partstore.Capabilities {
	return partstore.NewCapabilities(partstore.CapabilityTxFreeDeletePart)
}

func (s *failingTxFreeDeleteStore) DeletePart(ctx context.Context, tx database.Tx, id partstore.PartId) error {
	return errors.New("delete failed")
}

func TestSweepMetrics(t *testing.T) {
	testutils.SkipIfIntegration(t)
	forEachGCBackend(t, func(t *testing.T, env *gcTestEnv) {
		ctx := context.Background()
		fs, err := filesystem.New(t.TempDir())
		require.NoError(t, err)
		require.NoError(t, fs.Start(ctx))
		t.Cleanup(func() { require.NoError(t, fs.Stop(ctx)) })
		env.collector.partStores, err = partstore.NewNamedPartStores(env.physicalStore, map[string]partstore.PartStore{"flaky": &failingTxFreeDeleteStore{PartStore: fs}}, nil)
		require.NoError(t, err)
		flaky, gone := "flaky", "gone"
		failing, unknown := oldPartId(), oldPartId()
		require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
			require.NoError(t, fs.PutPart(ctx, tx, failing, bytes.NewBufferString("data")))
			require.NoError(t, env.registryRepo.RegisterUnreferenced(ctx, tx.SqlTx(), failing, &flaky))
			return env.registryRepo.RegisterUnreferenced(ctx, tx.SqlTx(), unknown, &gone)
		}))
		failuresBefore := metricValue(t, gcMetrics.deleteFailures.WithLabelValues(flaky))

		require.NoError(t, env.collector.runSweepWithContext(ctx))
		require.Equal(t, failuresBefore+1, metricValue(t, gcMetrics.deleteFailures.WithLabelValues(flaky)))
		require.EqualValues(t, 1, metricValue(t, gcMetrics.unknownStoreParts))
		require.NotZero(t, metricValue(t, gcMetrics.lastSuccess.WithLabelValues(passSweep)))
	})
}

func TestFullPassRepairMetrics(t *testing.T) {
	testutils.SkipIfIntegration(t)
	forEachGCBackend(t, func(t *testing.T, env *gcTestEnv) {
		ctx := context.Background()
		missing, miscounted := oldPartId(), oldPartId()
		bucket := metadatastore.MustNewBucketName("bucket")
		require.NoError(t, database.WithTx(ctx, env.db, nil, func(ctx context.Context, tx database.Tx) error {
			require.NoError(t, env.metadataStore.CreateBucket(ctx, tx.SqlTx(), bucket))
			for key, id := range map[string]partstore.PartId{"missing": missing, "miscounted": miscounted} {
				err := env.metadataStore.PutObject(ctx, tx.SqlTx(), bucket, &metadatastore.Object{Key: metadatastore.MustNewObjectKey(key), Size: 1, Parts: []metadatastore.Part{{Id: id, Size: 1}}}, nil)
				require.NoError(t, err)
			}
			_, err := tx.SqlTx().ExecContext(ctx, "DELETE FROM part_registry WHERE part_id = $1", missing.String())
			require.NoError(t, err)
			_, err = tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET ref_count = 5 WHERE part_id = $1", miscounted.String())
			return err
		}))
		restoredBefore := metricValue(t, gcMetrics.repairs.WithLabelValues(repairRegistryRestored))
		recountedBefore := metricValue(t, gcMetrics.repairs.WithLabelValues(repairRegistryRecounted))

		require.NoError(t, env.collector.runFullPassWithContext(ctx))
		require.Equal(t, restoredBefore+1, metricValue(t, gcMetrics.repairs.WithLabelValues(repairRegistryRestored)))
		require.Equal(t, recountedBefore+1, metricValue(t, gcMetrics.repairs.WithLabelValues(repairRegistryRecounted)))
		require.NotZero(t, metricValue(t, gcMetrics.lastSuccess.WithLabelValues(passFull)))
	})
}

func metricValue(t *testing.T, m prometheus.Metric) float64 {
	t.Helper()
	var out dto.Metric
	require.NoError(t, m.Write(&out))
	switch {
	case out.Counter != nil:
		return out.Counter.GetValue()
	case out.Gauge != nil:
		return out.Gauge.GetValue()
	}
	t.Fatalf("unsupported metric type")
	return 0
}
