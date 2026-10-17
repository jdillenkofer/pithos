package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"

	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/prometheus/client_golang/prometheus"
)

var bucketOnce sync.Once
var bucketRefreshMu sync.Mutex
var bucketBytes, bucketObjects *prometheus.GaugeVec

// RefreshBucketUsage samples current completed objects, excluding historical
// versions and delete markers. Database labels prevent mirrored stores from
// being silently conflated. Unsupported database schemas report an error rather
// than presenting a misleading zero.
func RefreshBucketUsage(ctx context.Context, dbs []database.Database) error {
	bucketRefreshMu.Lock()
	defer bucketRefreshMu.Unlock()
	bucketOnce.Do(func() {
		bucketBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "pithos_storage_bucket_bytes", Help: "Bytes in current live objects per bucket and database."}, []string{"database", "bucket"})
		bucketObjects = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "pithos_storage_bucket_objects", Help: "Current live objects per bucket and database."}, []string{"database", "bucket"})
	})
	pithosmetrics.Register(bucketBytes, bucketObjects)
	type usage struct {
		db        string
		aggregate object.BucketAggregate
	}
	var values []usage
	for i, db := range dbs {
		objectRepository, err := repositoryfactory.NewObjectRepository(db)
		if err != nil {
			return fmt.Errorf("bucket metrics database %d: %w", i, err)
		}
		err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			aggregates, err := objectRepository.GroupLiveObjectsByBucket(ctx, tx.SqlTx())
			if err != nil {
				return err
			}
			for _, aggregate := range aggregates {
				values = append(values, usage{db: strconv.Itoa(i), aggregate: aggregate})
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("bucket metrics database %d: %w", i, err)
		}
	}
	bucketBytes.Reset()
	bucketObjects.Reset()
	for _, v := range values {
		bucketBytes.WithLabelValues(v.db, v.aggregate.BucketName).Set(float64(v.aggregate.Size))
		bucketObjects.WithLabelValues(v.db, v.aggregate.BucketName).Set(float64(v.aggregate.Count))
	}
	return nil
}
