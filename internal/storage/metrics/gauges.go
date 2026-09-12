package metrics

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/prometheus/client_golang/prometheus"
)

var once sync.Once
var gauges struct {
	logicalParts, physicalParts prometheus.Gauge
	logicalBytes, physicalBytes prometheus.Gauge
	dedupRatio                  prometheus.Gauge
	physicalPartsByStore        *prometheus.GaugeVec
	physicalBytesByStore        *prometheus.GaugeVec
}

func register() {
	once.Do(func() {
		gauges.logicalParts = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "storage", Name: "parts_logical_total", Help: "Number of logical part references"})
		gauges.physicalParts = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "storage", Name: "parts_physical_total", Help: "Number of deduplicated physical parts"})
		gauges.logicalBytes = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "storage", Name: "part_bytes_logical", Help: "Logical bytes referenced by parts"})
		gauges.physicalBytes = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "storage", Name: "part_bytes_physical", Help: "Bytes represented by deduplicated physical parts"})
		gauges.dedupRatio = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "storage", Name: "dedup_ratio", Help: "Ratio of logical parts to physical parts"})
		gauges.physicalPartsByStore = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "storage", Name: "parts_physical_by_store", Help: "Number of physical parts by part store"}, []string{"store"})
		gauges.physicalBytesByStore = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "storage", Name: "part_bytes_physical_by_store", Help: "Physical part bytes by part store"}, []string{"store"})
	})
	pithosmetrics.Register(gauges.logicalParts, gauges.physicalParts, gauges.logicalBytes, gauges.physicalBytes, gauges.dedupRatio, gauges.physicalPartsByStore, gauges.physicalBytesByStore)
}

func refresh(ctx context.Context, dbs []database.Database) error {
	var logicalParts, physicalParts, logicalBytes, physicalBytes int64
	partsByStore := map[string]int64{}
	bytesByStore := map[string]int64{}
	for _, db := range dbs {
		partRepository, err := repositoryfactory.NewPartRepository(db)
		if err != nil {
			return err
		}
		dedupRepository, err := repositoryfactory.NewPartDedupIndexRepository(db)
		if err != nil {
			return err
		}
		err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			logical, err := partRepository.GroupByStore(ctx, tx.SqlTx())
			if err != nil {
				return err
			}
			physical, err := dedupRepository.GroupByStore(ctx, tx.SqlTx())
			if err != nil {
				return err
			}
			for _, a := range logical {
				logicalParts += a.Count
				logicalBytes += a.Size
			}
			for _, a := range physical {
				physicalParts += a.Count
				physicalBytes += a.Size
				partsByStore[a.Store] += a.Count
				bytesByStore[a.Store] += a.Size
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	gauges.logicalParts.Set(float64(logicalParts))
	gauges.physicalParts.Set(float64(physicalParts))
	gauges.logicalBytes.Set(float64(logicalBytes))
	gauges.physicalBytes.Set(float64(physicalBytes))
	ratio := float64(0)
	if physicalParts > 0 {
		ratio = float64(logicalParts) / float64(physicalParts)
	}
	gauges.dedupRatio.Set(ratio)
	gauges.physicalPartsByStore.Reset()
	gauges.physicalBytesByStore.Reset()
	for store, count := range partsByStore {
		gauges.physicalPartsByStore.WithLabelValues(store).Set(float64(count))
		gauges.physicalBytesByStore.WithLabelValues(store).Set(float64(bytesByStore[store]))
	}
	return nil
}

func Start(dbs []database.Database, interval time.Duration) *task.TaskHandle {
	register()
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return task.Start(func(cancel *atomic.Bool) {
		for !cancel.Load() {
			started := time.Now()
			err := refresh(context.Background(), dbs)
			pithosmetrics.ObserveRefresh("storage_database", started, err)
			if err != nil {
				slog.Warn("Could not refresh storage database metrics", "error", err)
			}
			deadline := time.Now().Add(interval)
			for !cancel.Load() && time.Now().Before(deadline) {
				time.Sleep(min(250*time.Millisecond, time.Until(deadline)))
			}
		}
	})
}
