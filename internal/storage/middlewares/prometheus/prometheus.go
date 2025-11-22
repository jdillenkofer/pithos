package prometheus

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/prometheus/client_golang/prometheus"
)

type prometheusStorageMiddleware struct {
	*lifecycle.ValidatedLifecycle
	registerer                   prometheus.Registerer
	failedApiOpsCounter          *prometheus.CounterVec
	successfulApiOpsCounter      *prometheus.CounterVec
	totalSizeByBucket            *prometheus.GaugeVec
	totalBytesUploadedByBucket   *prometheus.CounterVec
	totalBytesDownloadedByBucket *prometheus.CounterVec
	metricsMeasuringTaskHandle   *task.TaskHandle
	innerStorage                 storage.Storage
}

// Compile-time check to ensure prometheusStorageMiddleware implements storage.Storage
var _ storage.Storage = (*prometheusStorageMiddleware)(nil)

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

	lifecycle, err := lifecycle.NewValidatedLifecycle("PrometheusStorageMiddleware")
	if err != nil {
		return nil, err
	}

	return &prometheusStorageMiddleware{
		ValidatedLifecycle:           lifecycle,
		registerer:                   registerer,
		failedApiOpsCounter:          failedApiOpsCounter,
		successfulApiOpsCounter:      successfulApiOpsCounter,
		totalSizeByBucket:            totalSizeByBucket,
		totalBytesUploadedByBucket:   totalBytesUploadedByBucket,
		totalBytesDownloadedByBucket: totalBytesDownloadedByBucket,
		innerStorage:                 innerStorage,
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
		psm.totalSizeByBucket.With(prometheus.Labels{"bucket": bucket.Name.String()}).Set(float64(*totalSize))
	}
}

func (psm *prometheusStorageMiddleware) getTotalSizeByBucket(ctx context.Context, bucket storage.Bucket) (*int64, error) {
	var totalSize int64 = 0
	var startAfter *string
	truncated := true

	for truncated {
		listBucketResult, err := psm.innerStorage.ListObjects(ctx, bucket.Name, storage.ListObjectsOptions{
			StartAfter: startAfter,
			MaxKeys:    1000,
		})
		if err != nil {
			return nil, err
		}
		for _, object := range listBucketResult.Objects {
			totalSize += object.Size
		}
		truncated = listBucketResult.IsTruncated
		if len(listBucketResult.Objects) > 0 {
			startAfter = ptrutils.ToPtr(listBucketResult.Objects[len(listBucketResult.Objects)-1].Key.String())
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
	if err := psm.ValidatedLifecycle.Start(ctx); err != nil {
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

	return psm.innerStorage.Start(ctx)
}

func (psm *prometheusStorageMiddleware) Stop(ctx context.Context) error {
	if err := psm.ValidatedLifecycle.Stop(ctx); err != nil {
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
			slog.Debug("PrometheusStorageMiddleware.metricsMeasuringTaskHandle joined with timeout of 30s")
		} else {
			slog.Debug("PrometheusStorageMiddleware.metricsMeasuringTaskHandle joined without timeout")
		}
	}

	return psm.innerStorage.Stop(ctx)
}

func (psm *prometheusStorageMiddleware) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	err := psm.innerStorage.CreateBucket(ctx, bucketName)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "CreateBucket"}).Inc()

		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "CreateBucket"}).Inc()

	return nil
}

func (psm *prometheusStorageMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	err := psm.innerStorage.DeleteBucket(ctx, bucketName)
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

func (psm *prometheusStorageMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	mBucket, err := psm.innerStorage.HeadBucket(ctx, bucketName)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "HeadBucket"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "HeadBucket"}).Inc()

	return mBucket, err
}

func (psm *prometheusStorageMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	mListBucketResult, err := psm.innerStorage.ListObjects(ctx, bucketName, opts)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "ListObjects"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "ListObjects"}).Inc()

	return mListBucketResult, nil
}

func (psm *prometheusStorageMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (*storage.Object, error) {
	mObject, err := psm.innerStorage.HeadObject(ctx, bucketName, key)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "HeadObject"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "HeadObject"}).Inc()

	return mObject, nil
}

func (psm *prometheusStorageMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	reader, err := psm.innerStorage.GetObject(ctx, bucketName, key, startByte, endByte)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "GetObject"}).Inc()
		return nil, err
	}

	reader = ioutils.NewStatsReadCloser(reader, func(n int) {
		psm.totalBytesDownloadedByBucket.With(prometheus.Labels{"bucket": bucketName.String()}).Add(float64(n))
	})

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "GetObject"}).Inc()

	return reader, nil
}

func (psm *prometheusStorageMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	reader = ioutils.NewStatsReadCloser(ioutils.NewNopSeekCloser(reader), func(n int) {
		psm.totalBytesUploadedByBucket.With(prometheus.Labels{"bucket": bucketName.String()}).Add(float64(n))
	})

	putObjectResult, err := psm.innerStorage.PutObject(ctx, bucketName, key, contentType, reader, checksumInput)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "PutObject"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "PutObject"}).Inc()

	return putObjectResult, nil
}

func (psm *prometheusStorageMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	err := psm.innerStorage.DeleteObject(ctx, bucketName, key)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "DeleteObject"}).Inc()
		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "DeleteObject"}).Inc()

	return nil
}

func (psm *prometheusStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	initiateMultipartUploadResult, err := psm.innerStorage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "CreateMultipartUpload"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "CreateMultipartUpload"}).Inc()

	return initiateMultipartUploadResult, nil
}

func (psm *prometheusStorageMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	bytesUploaded := 0
	data = ioutils.NewStatsReadCloser(ioutils.NewNopSeekCloser(data), func(n int) {
		bytesUploaded += n
	})

	uploadPartResult, err := psm.innerStorage.UploadPart(ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "UploadPart"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "UploadPart"}).Inc()
	psm.totalBytesUploadedByBucket.With(prometheus.Labels{"bucket": bucketName.String()}).Add(float64(bytesUploaded))

	return uploadPartResult, nil
}

func (psm *prometheusStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	completeMultipartUploadResult, err := psm.innerStorage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "CompleteMultipartUpload"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "CompleteMultipartUpload"}).Inc()

	return completeMultipartUploadResult, nil
}

func (psm *prometheusStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	err := psm.innerStorage.AbortMultipartUpload(ctx, bucketName, key, uploadId)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "AbortMultipartUpload"}).Inc()
		return err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "AbortMultipartUpload"}).Inc()

	return nil
}

func (psm *prometheusStorageMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	listMultipartUploadsResult, err := psm.innerStorage.ListMultipartUploads(ctx, bucketName, opts)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "ListMultipartUploads"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "ListMultipartUploads"}).Inc()
	return listMultipartUploadsResult, nil
}

func (psm *prometheusStorageMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	listPartsResult, err := psm.innerStorage.ListParts(ctx, bucketName, key, uploadId, opts)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "ListParts"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "ListParts"}).Inc()
	return listPartsResult, nil
}
