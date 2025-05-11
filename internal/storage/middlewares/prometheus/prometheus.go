package prometheus

import (
	"context"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/startstopvalidator"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/prometheus/client_golang/prometheus"
)

type prometheusStorageMiddleware struct {
	registerer                   prometheus.Registerer
	failedApiOpsCounter          *prometheus.CounterVec
	successfulApiOpsCounter      *prometheus.CounterVec
	totalSizeByBucket            *prometheus.GaugeVec
	totalBytesUploadedByBucket   *prometheus.CounterVec
	totalBytesDownloadedByBucket *prometheus.CounterVec
	metricsMeasuringTaskHandle   *task.TaskHandle
	innerStorage                 storage.Storage
	startStopValidator           *startstopvalidator.StartStopValidator
}

func NewStorageMiddleware(innerStorage storage.Storage, registerer prometheus.Registerer) (storage.Storage, error) {
	failedApiOpsCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "storage",
			Name:      "failed_api_ops_total",
			Help:      "No of failed api operations handled by Pithos partitioned by type",
		},
		[]string{"type"},
	)

	successfulApiOpsCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "storage",
			Name:      "successful_api_ops_total",
			Help:      "No of successful api operations handled by Pithos partitioned by type",
		},
		[]string{"type"},
	)

	totalSizeByBucket := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pithos",
			Subsystem: "storage",
			Name:      "total_size",
			Help:      "Total size by bucket",
		},
		[]string{"bucket"},
	)

	totalBytesUploadedByBucket := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "storage",
			Name:      "bytes_uploaded_total",
			Help:      "Total bytes uploaded by bucket",
		},
		[]string{"bucket"},
	)

	totalBytesDownloadedByBucket := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "storage",
			Name:      "bytes_downloaded_total",
			Help:      "Total bytes downloaded by bucket",
		},
		[]string{"bucket"},
	)

	startStopValidator, err := startstopvalidator.New("PrometheusStorageMiddleware")
	if err != nil {
		return nil, err
	}

	return &prometheusStorageMiddleware{
		registerer:                   registerer,
		failedApiOpsCounter:          failedApiOpsCounter,
		successfulApiOpsCounter:      successfulApiOpsCounter,
		totalSizeByBucket:            totalSizeByBucket,
		totalBytesUploadedByBucket:   totalBytesUploadedByBucket,
		totalBytesDownloadedByBucket: totalBytesDownloadedByBucket,
		innerStorage:                 innerStorage,
		startStopValidator:           startStopValidator,
	}, nil
}

func (psm *prometheusStorageMiddleware) measureMetrics(ctx context.Context) {
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

func (psm *prometheusStorageMiddleware) getTotalSizeByBucket(ctx context.Context, bucket storage.Bucket) (*int64, error) {
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

func (psm *prometheusStorageMiddleware) measureMetricsLoop(cancelMetricsMeasuring *atomic.Bool) {
	ctx := context.Background()
	for {
		psm.measureMetrics(ctx)
		for range 30 * 4 {
			time.Sleep(250 * time.Millisecond)
			if cancelMetricsMeasuring.Load() {
				return
			}
		}
	}
}

func (psm *prometheusStorageMiddleware) Start(ctx context.Context) error {
	err := psm.startStopValidator.Start()
	if err != nil {
		return err
	}
	psm.registerer.MustRegister(psm.failedApiOpsCounter)
	psm.registerer.MustRegister(psm.successfulApiOpsCounter)
	psm.registerer.MustRegister(psm.totalSizeByBucket)
	psm.registerer.MustRegister(psm.totalBytesUploadedByBucket)
	psm.registerer.MustRegister(psm.totalBytesDownloadedByBucket)

	psm.metricsMeasuringTaskHandle = task.Start(func(cancelTask *atomic.Bool) {
		psm.measureMetricsLoop(cancelTask)
	})

	err = psm.innerStorage.Start(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (psm *prometheusStorageMiddleware) Stop(ctx context.Context) error {
	err := psm.startStopValidator.Stop()
	if err != nil {
		return err
	}

	psm.registerer.Unregister(psm.totalBytesDownloadedByBucket)
	psm.registerer.Unregister(psm.totalBytesUploadedByBucket)
	psm.registerer.Unregister(psm.totalSizeByBucket)
	psm.registerer.Unregister(psm.successfulApiOpsCounter)
	psm.registerer.Unregister(psm.failedApiOpsCounter)

	if psm.metricsMeasuringTaskHandle != nil && !psm.metricsMeasuringTaskHandle.IsCancelled() {
		psm.metricsMeasuringTaskHandle.Cancel()
		joinedWithTimeout := psm.metricsMeasuringTaskHandle.JoinWithTimeout(30 * time.Second)
		if joinedWithTimeout {
			log.Println("PrometheusStorageMiddleware.metricsMeasuringTaskHandle joined with timeout of 30s")
		} else {
			log.Println("PrometheusStorageMiddleware.metricsMeasuringTaskHandle joined without timeout")
		}
	}

	err = psm.innerStorage.Stop(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (psm *prometheusStorageMiddleware) CreateBucket(ctx context.Context, bucket string) error {
	err := psm.innerStorage.CreateBucket(ctx, bucket)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "CreateBucket"}).Inc()

		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "CreateBucket"}).Inc()

	return nil
}

func (psm *prometheusStorageMiddleware) DeleteBucket(ctx context.Context, bucket string) error {
	err := psm.innerStorage.DeleteBucket(ctx, bucket)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "DeleteBucket"}).Inc()
		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "DeleteBucket"}).Inc()

	return nil
}

func (psm *prometheusStorageMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	mBuckets, err := psm.innerStorage.ListBuckets(ctx)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "ListBuckets"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "ListBuckets"}).Inc()

	return mBuckets, nil
}

func (psm *prometheusStorageMiddleware) HeadBucket(ctx context.Context, bucket string) (*storage.Bucket, error) {
	mBucket, err := psm.innerStorage.HeadBucket(ctx, bucket)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "HeadBucket"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "HeadBucket"}).Inc()

	return mBucket, err
}

func (psm *prometheusStorageMiddleware) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*storage.ListBucketResult, error) {
	mListBucketResult, err := psm.innerStorage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "ListObjects"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "ListObjects"}).Inc()

	return mListBucketResult, nil
}

func (psm *prometheusStorageMiddleware) HeadObject(ctx context.Context, bucket string, key string) (*storage.Object, error) {
	mObject, err := psm.innerStorage.HeadObject(ctx, bucket, key)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "HeadObject"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "HeadObject"}).Inc()

	return mObject, nil
}

func (psm *prometheusStorageMiddleware) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	reader, err := psm.innerStorage.GetObject(ctx, bucket, key, startByte, endByte)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "GetObject"}).Inc()
		return nil, err
	}

	reader = ioutils.NewStatsReadCloser(reader, func(n int) {
		psm.totalBytesDownloadedByBucket.With(prometheus.Labels{"bucket": bucket}).Add(float64(n))
	})

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "GetObject"}).Inc()

	return reader, nil
}

func (psm *prometheusStorageMiddleware) PutObject(ctx context.Context, bucket string, key string, contentType string, reader io.Reader) error {
	reader = ioutils.NewStatsReadCloser(ioutils.NewNopSeekCloser(reader), func(n int) {
		psm.totalBytesUploadedByBucket.With(prometheus.Labels{"bucket": bucket}).Add(float64(n))
	})

	err := psm.innerStorage.PutObject(ctx, bucket, key, contentType, reader)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "PutObject"}).Inc()
		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "PutObject"}).Inc()

	return nil
}

func (psm *prometheusStorageMiddleware) DeleteObject(ctx context.Context, bucket string, key string) error {
	err := psm.innerStorage.DeleteObject(ctx, bucket, key)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "DeleteObject"}).Inc()
		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "DeleteObject"}).Inc()

	return nil
}

func (psm *prometheusStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucket string, key string, contentType string) (*storage.InitiateMultipartUploadResult, error) {
	initiateMultipartUploadResult, err := psm.innerStorage.CreateMultipartUpload(ctx, bucket, key, contentType)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "CreateMultipartUpload"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "CreateMultipartUpload"}).Inc()

	return initiateMultipartUploadResult, nil
}

func (psm *prometheusStorageMiddleware) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, data io.Reader) (*storage.UploadPartResult, error) {
	bytesUploaded := 0
	data = ioutils.NewStatsReadCloser(ioutils.NewNopSeekCloser(data), func(n int) {
		bytesUploaded += n
	})

	uploadPartResult, err := psm.innerStorage.UploadPart(ctx, bucket, key, uploadId, partNumber, data)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "UploadPart"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "UploadPart"}).Inc()
	psm.totalBytesUploadedByBucket.With(prometheus.Labels{"bucket": bucket}).Add(float64(bytesUploaded))

	return uploadPartResult, nil
}

func (psm *prometheusStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*storage.CompleteMultipartUploadResult, error) {
	completeMultipartUploadResult, err := psm.innerStorage.CompleteMultipartUpload(ctx, bucket, key, uploadId)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "CompleteMultipartUpload"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "CompleteMultipartUpload"}).Inc()

	return completeMultipartUploadResult, nil
}

func (psm *prometheusStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	err := psm.innerStorage.AbortMultipartUpload(ctx, bucket, key, uploadId)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "AbortMultipartUpload"}).Inc()
		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "AbortMultipartUpload"}).Inc()

	return nil
}
