package storage

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusStorageMiddleware struct {
	registerer              prometheus.Registerer
	failedApiOpsCounter     *prometheus.CounterVec
	successfulApiOpsCounter *prometheus.CounterVec
	totalSizeByBucket       *prometheus.GaugeVec
	metricsMeasuringStopped sync.WaitGroup
	stopMetricsMeasuring    atomic.Bool
	innerStorage            Storage
}

func NewPrometheusStorageMiddleware(innerStorage Storage, registerer prometheus.Registerer) (*PrometheusStorageMiddleware, error) {
	failedApiOpsCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "storage",
			Name:      "pithos_failed_api_ops_total",
			Help:      "No of failed api operations handled by Pithos partitioned by type",
		},
		[]string{"type"},
	)

	successfulApiOpsCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "storage",
			Name:      "pithos_successful_api_ops_total",
			Help:      "No of successful api operations handled by Pithos partitioned by type",
		},
		[]string{"type"},
	)

	totalSizeByBucket := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pithos",
			Subsystem: "storage",
			Name:      "pithos_total_size",
			Help:      "Total size by bucket",
		},
		[]string{"bucket"},
	)

	return &PrometheusStorageMiddleware{
		registerer:              registerer,
		failedApiOpsCounter:     failedApiOpsCounter,
		successfulApiOpsCounter: successfulApiOpsCounter,
		totalSizeByBucket:       totalSizeByBucket,
		stopMetricsMeasuring:    atomic.Bool{},
		innerStorage:            innerStorage,
	}, nil
}

func (psm *PrometheusStorageMiddleware) measureMetrics(ctx context.Context) {
	buckets, err := psm.innerStorage.ListBuckets(ctx)
	if err != nil {
		return
	}
	for _, bucket := range buckets {
		totalSize, err := psm.getTotalSizeByBucket(ctx, bucket)
		if err != nil {
			return
		}
		psm.totalSizeByBucket.With(prometheus.Labels{"bucket": bucket.Name}).Set(float64(*totalSize))
	}
}

func (psm *PrometheusStorageMiddleware) getTotalSizeByBucket(ctx context.Context, bucket Bucket) (*int64, error) {
	var totalSize int64 = 0
	var startAfter string = ""
	truncated := true

	for truncated {
		listBucketResult, err := psm.innerStorage.ListObjects(ctx, bucket.Name, "", "", startAfter, 1000)
		if err != nil {
			return nil, err
		}
		for _, object := range listBucketResult.Objects {
			totalSize += object.Size
		}
		truncated = listBucketResult.IsTruncated
		if len(listBucketResult.Objects) > 0 {
			startAfter = listBucketResult.Objects[len(listBucketResult.Objects)-1].Key
		}
	}
	return &totalSize, nil
}

func (psm *PrometheusStorageMiddleware) measureMetricsLoop() {
	ctx := context.Background()
out:
	for {
		psm.measureMetrics(ctx)
		for range 30 * 4 {
			time.Sleep(250 * time.Millisecond)
			if psm.stopMetricsMeasuring.Load() {
				break out
			}
		}
	}
	psm.metricsMeasuringStopped.Done()
}

func (psm *PrometheusStorageMiddleware) Start(ctx context.Context) error {
	psm.registerer.MustRegister(psm.failedApiOpsCounter)
	psm.registerer.MustRegister(psm.successfulApiOpsCounter)
	psm.registerer.MustRegister(psm.totalSizeByBucket)

	psm.metricsMeasuringStopped.Add(1)
	go psm.measureMetricsLoop()

	err := psm.innerStorage.Start(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (psm *PrometheusStorageMiddleware) Stop(ctx context.Context) error {
	psm.registerer.Unregister(psm.totalSizeByBucket)
	psm.registerer.Unregister(psm.successfulApiOpsCounter)
	psm.registerer.Unregister(psm.failedApiOpsCounter)

	if !psm.stopMetricsMeasuring.Load() {
		psm.stopMetricsMeasuring.Store(true)
		waitWithTimeout(&psm.metricsMeasuringStopped, 5*time.Second)
	}

	err := psm.innerStorage.Stop(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (psm *PrometheusStorageMiddleware) CreateBucket(ctx context.Context, bucket string) error {
	err := psm.innerStorage.CreateBucket(ctx, bucket)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "CreateBucket"}).Inc()

		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "CreateBucket"}).Inc()

	return nil
}

func (psm *PrometheusStorageMiddleware) DeleteBucket(ctx context.Context, bucket string) error {
	err := psm.innerStorage.DeleteBucket(ctx, bucket)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "DeleteBucket"}).Inc()
		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "DeleteBucket"}).Inc()

	return nil
}

func (psm *PrometheusStorageMiddleware) ListBuckets(ctx context.Context) ([]Bucket, error) {
	mBuckets, err := psm.innerStorage.ListBuckets(ctx)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "ListBuckets"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "ListBuckets"}).Inc()

	return mBuckets, nil
}

func (psm *PrometheusStorageMiddleware) HeadBucket(ctx context.Context, bucket string) (*Bucket, error) {
	mBucket, err := psm.innerStorage.HeadBucket(ctx, bucket)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "HeadBucket"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "HeadBucket"}).Inc()

	return mBucket, err
}

func (psm *PrometheusStorageMiddleware) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	mListBucketResult, err := psm.innerStorage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "ListObjects"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "ListObjects"}).Inc()

	return mListBucketResult, nil
}

func (psm *PrometheusStorageMiddleware) HeadObject(ctx context.Context, bucket string, key string) (*Object, error) {
	mObject, err := psm.innerStorage.HeadObject(ctx, bucket, key)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "HeadObject"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "HeadObject"}).Inc()

	return mObject, nil
}

func (psm *PrometheusStorageMiddleware) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	reader, err := psm.innerStorage.GetObject(ctx, bucket, key, startByte, endByte)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "GetObject"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "GetObject"}).Inc()

	return reader, nil
}

func (psm *PrometheusStorageMiddleware) PutObject(ctx context.Context, bucket string, key string, reader io.Reader) error {
	err := psm.innerStorage.PutObject(ctx, bucket, key, reader)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "PutObject"}).Inc()
		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "PutObject"}).Inc()

	return nil
}

func (psm *PrometheusStorageMiddleware) DeleteObject(ctx context.Context, bucket string, key string) error {
	err := psm.innerStorage.DeleteObject(ctx, bucket, key)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "DeleteObject"}).Inc()
		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "DeleteObject"}).Inc()

	return nil
}

func (psm *PrometheusStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucket string, key string) (*InitiateMultipartUploadResult, error) {
	initiateMultipartUploadResult, err := psm.innerStorage.CreateMultipartUpload(ctx, bucket, key)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "CreateMultipartUpload"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "CreateMultipartUpload"}).Inc()

	return initiateMultipartUploadResult, nil
}

func (psm *PrometheusStorageMiddleware) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, data io.Reader) (*UploadPartResult, error) {
	uploadPartResult, err := psm.innerStorage.UploadPart(ctx, bucket, key, uploadId, partNumber, data)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "UploadPart"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "UploadPart"}).Inc()

	return uploadPartResult, nil
}

func (psm *PrometheusStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	completeMultipartUploadResult, err := psm.innerStorage.CompleteMultipartUpload(ctx, bucket, key, uploadId)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "CompleteMultipartUpload"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "CompleteMultipartUpload"}).Inc()

	return completeMultipartUploadResult, nil
}

func (psm *PrometheusStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	err := psm.innerStorage.AbortMultipartUpload(ctx, bucket, key, uploadId)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "AbortMultipartUpload"}).Inc()
		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "AbortMultipartUpload"}).Inc()

	return nil
}
