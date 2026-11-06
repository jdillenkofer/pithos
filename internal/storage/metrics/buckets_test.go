package metrics

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repository "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	metadatasql "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func gaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, gauge.Write(&metric))
	return metric.GetGauge().GetValue()
}

func TestRefreshBucketUsageCountsOnlyCurrentLiveObjects(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "db.sqlite"))
	require.NoError(t, err)
	defer db.Close()
	buckets, err := repository.NewBucketRepository(db)
	require.NoError(t, err)
	objects, err := repository.NewObjectRepository(db)
	require.NoError(t, err)
	parts, err := repository.NewPartRepository(db)
	require.NoError(t, err)
	tags, err := repository.NewTagRepository(db)
	require.NoError(t, err)
	metadata, err := repository.NewUserMetadataRepository(db)
	require.NoError(t, err)
	ms, err := metadatasql.New(db, buckets, objects, parts, tags, metadata)
	require.NoError(t, err)
	partStore, err := filesystem.New(t.TempDir())
	require.NoError(t, err)
	live, err := metadatapart.NewStorageWithNamedPartStores(db, ms, partStore, nil, nil)
	require.NoError(t, err)
	ctx := t.Context()
	require.NoError(t, live.Start(ctx))
	defer live.Stop(context.Background())

	empty := storage.MustNewBucketName("empty")
	versioned := storage.MustNewBucketName("versioned")
	require.NoError(t, live.CreateBucket(ctx, empty))
	require.NoError(t, live.CreateBucket(ctx, versioned))
	require.NoError(t, live.PutBucketVersioningConfiguration(ctx, versioned, &storage.BucketVersioningConfiguration{Status: ptrutils.ToPtr(storage.BucketVersioningStatusEnabled)}))
	put := func(key, content string) {
		t.Helper()
		_, err := live.PutObject(ctx, versioned, storage.MustNewObjectKey(key), nil, bytes.NewBufferString(content), nil, nil)
		require.NoError(t, err)
	}
	put("current", "old")
	put("current", "current")
	put("deleted", "gone")
	_, err = live.DeleteObject(ctx, versioned, storage.MustNewObjectKey("deleted"), nil)
	require.NoError(t, err)

	require.NoError(t, RefreshBucketUsage(ctx, []database.Database{db}))
	require.Equal(t, float64(len("current")), gaugeValue(t, bucketBytes.WithLabelValues("0", "versioned")))
	require.Equal(t, float64(1), gaugeValue(t, bucketObjects.WithLabelValues("0", "versioned")))
	require.Zero(t, gaugeValue(t, bucketBytes.WithLabelValues("0", "empty")))
	require.Zero(t, gaugeValue(t, bucketObjects.WithLabelValues("0", "empty")))

	require.NoError(t, db.Close())
	require.Error(t, RefreshBucketUsage(ctx, []database.Database{db}))
	require.Equal(t, float64(1), gaugeValue(t, bucketObjects.WithLabelValues("0", "versioned")), "a failed refresh must keep the last sample")
}
