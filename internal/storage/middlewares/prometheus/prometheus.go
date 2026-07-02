package prometheus

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type prometheusStorageMiddleware struct {
	*lifecycle.ValidatedLifecycle
	delegator.DelegatingStorage
	registerer                   prometheus.Registerer
	failedApiOpsCounter          *prometheus.CounterVec
	successfulApiOpsCounter      *prometheus.CounterVec
	totalSizeByBucket            *prometheus.GaugeVec
	totalBytesUploadedByBucket   *prometheus.CounterVec
	totalBytesDownloadedByBucket *prometheus.CounterVec
	metricsMeasuringTaskHandle   *task.TaskHandle
	tracer                       trace.Tracer
}

func (psm *prometheusStorageMiddleware) run(ctx context.Context, spanName string, opType string, fn func(context.Context) error) error {
	ctx, span := psm.tracer.Start(ctx, spanName)
	defer span.End()

	err := fn(ctx)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": opType}).Inc()
		return err
	}
	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": opType}).Inc()
	return nil
}

// Compile-time check to ensure prometheusStorageMiddleware implements storage.Storage
var _ storage.Storage = (*prometheusStorageMiddleware)(nil)
var _ storage.TransactionalStorage = (*prometheusStorageMiddleware)(nil)

func (psm *prometheusStorageMiddleware) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return delegator.WithTransaction(ctx, opts, psm.Next, psm, fn)
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

	lifecycle, err := lifecycle.NewValidatedLifecycle("PrometheusStorageMiddleware")
	if err != nil {
		return nil, err
	}

	return &prometheusStorageMiddleware{
		ValidatedLifecycle:           lifecycle,
		DelegatingStorage:            delegator.Wrap(innerStorage),
		registerer:                   registerer,
		failedApiOpsCounter:          failedApiOpsCounter,
		successfulApiOpsCounter:      successfulApiOpsCounter,
		totalSizeByBucket:            totalSizeByBucket,
		totalBytesUploadedByBucket:   totalBytesUploadedByBucket,
		totalBytesDownloadedByBucket: totalBytesDownloadedByBucket,
		tracer:                       otel.Tracer("internal/storage/middlewares/prometheus"),
	}, nil
}

func (psm *prometheusStorageMiddleware) measureMetrics(ctx context.Context) {
	buckets, err := psm.Next.ListBuckets(ctx)
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
		listBucketResult, err := psm.Next.ListObjects(ctx, bucket.Name, storage.ListObjectsOptions{
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
	if err := psm.registerer.Register(psm.failedApiOpsCounter); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register failedApiOpsCounter metric", "error", err)
		}
	}
	if err := psm.registerer.Register(psm.successfulApiOpsCounter); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register successfulApiOpsCounter metric", "error", err)
		}
	}
	if err := psm.registerer.Register(psm.totalSizeByBucket); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register totalSizeByBucket metric", "error", err)
		}
	}
	if err := psm.registerer.Register(psm.totalBytesUploadedByBucket); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register totalBytesUploadedByBucket metric", "error", err)
		}
	}
	if err := psm.registerer.Register(psm.totalBytesDownloadedByBucket); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register totalBytesDownloadedByBucket metric", "error", err)
		}
	}

	psm.metricsMeasuringTaskHandle = task.Start(func(cancelTask *atomic.Bool) {
		psm.measureMetricsLoop(cancelTask)
	})

	return psm.Next.Start(ctx)
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

	return psm.Next.Stop(ctx)
}

func (psm *prometheusStorageMiddleware) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.CreateBucket", "CreateBucket", func(ctx context.Context) error {
		return psm.Next.CreateBucket(ctx, bucketName)
	})
}

func (psm *prometheusStorageMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.DeleteBucket", "DeleteBucket", func(ctx context.Context) error {
		return psm.Next.DeleteBucket(ctx, bucketName)
	})
}

func (psm *prometheusStorageMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	var result []storage.Bucket
	err := psm.run(ctx, "PrometheusStorageMiddleware.ListBuckets", "ListBuckets", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.ListBuckets(ctx)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	var result *storage.Bucket
	err := psm.run(ctx, "PrometheusStorageMiddleware.HeadBucket", "HeadBucket", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.HeadBucket(ctx, bucketName)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) GetBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.WebsiteConfiguration, error) {
	var result *storage.WebsiteConfiguration
	err := psm.run(ctx, "PrometheusStorageMiddleware.GetBucketWebsiteConfiguration", "GetBucketWebsite", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.GetBucketWebsiteConfiguration(ctx, bucketName)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.PutBucketWebsiteConfiguration", "PutBucketWebsite", func(ctx context.Context) error {
		return psm.Next.PutBucketWebsiteConfiguration(ctx, bucketName, config)
	})
}

func (psm *prometheusStorageMiddleware) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.DeleteBucketWebsiteConfiguration", "DeleteBucketWebsite", func(ctx context.Context) error {
		return psm.Next.DeleteBucketWebsiteConfiguration(ctx, bucketName)
	})
}

func (psm *prometheusStorageMiddleware) GetBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	var result *storage.BucketCORSConfiguration
	err := psm.run(ctx, "PrometheusStorageMiddleware.GetBucketCORSConfiguration", "GetBucketCORS", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.GetBucketCORSConfiguration(ctx, bucketName)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.PutBucketCORSConfiguration", "PutBucketCORS", func(ctx context.Context) error {
		return psm.Next.PutBucketCORSConfiguration(ctx, bucketName, config)
	})
}

func (psm *prometheusStorageMiddleware) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.DeleteBucketCORSConfiguration", "DeleteBucketCORS", func(ctx context.Context) error {
		return psm.Next.DeleteBucketCORSConfiguration(ctx, bucketName)
	})
}

func (psm *prometheusStorageMiddleware) GetBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketLifecycleConfiguration, error) {
	var result *storage.BucketLifecycleConfiguration
	err := psm.run(ctx, "PrometheusStorageMiddleware.GetBucketLifecycleConfiguration", "GetBucketLifecycle", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.GetBucketLifecycleConfiguration(ctx, bucketName)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) PutBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketLifecycleConfiguration) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.PutBucketLifecycleConfiguration", "PutBucketLifecycle", func(ctx context.Context) error {
		return psm.Next.PutBucketLifecycleConfiguration(ctx, bucketName, config)
	})
}

func (psm *prometheusStorageMiddleware) DeleteBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.DeleteBucketLifecycleConfiguration", "DeleteBucketLifecycle", func(ctx context.Context) error {
		return psm.Next.DeleteBucketLifecycleConfiguration(ctx, bucketName)
	})
}

func (psm *prometheusStorageMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	var result *storage.ListBucketResult
	err := psm.run(ctx, "PrometheusStorageMiddleware.ListObjects", "ListObjects", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.ListObjects(ctx, bucketName, opts)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	var result *storage.Object
	err := psm.run(ctx, "PrometheusStorageMiddleware.HeadObject", "HeadObject", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.HeadObject(ctx, bucketName, key, opts)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := psm.tracer.Start(ctx, "PrometheusStorageMiddleware.GetObject")
	defer span.End()

	object, readers, err := psm.Next.GetObject(ctx, bucketName, key, ranges, opts)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "GetObject"}).Inc()
		return nil, nil, err
	}

	// Wrap each reader with stats tracking
	for i, reader := range readers {
		readers[i] = ioutils.NewStatsReadCloser(reader, func(n int) {
			psm.totalBytesDownloadedByBucket.With(prometheus.Labels{"bucket": bucketName.String()}).Add(float64(n))
		})
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "GetObject"}).Inc()

	return object, readers, nil
}

func (psm *prometheusStorageMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	ctx, span := psm.tracer.Start(ctx, "PrometheusStorageMiddleware.PutObject")
	defer span.End()

	reader = ioutils.NewStatsReadCloser(ioutils.NewNopSeekCloser(reader), func(n int) {
		psm.totalBytesUploadedByBucket.With(prometheus.Labels{"bucket": bucketName.String()}).Add(float64(n))
	})

	putObjectResult, err := psm.Next.PutObject(ctx, bucketName, key, contentType, reader, checksumInput, opts)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "PutObject"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "PutObject"}).Inc()

	return putObjectResult, nil
}

func (psm *prometheusStorageMiddleware) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	ctx, span := psm.tracer.Start(ctx, "PrometheusStorageMiddleware.AppendObject")
	defer span.End()

	reader = ioutils.NewStatsReadCloser(ioutils.NewNopSeekCloser(reader), func(n int) {
		psm.totalBytesUploadedByBucket.With(prometheus.Labels{"bucket": bucketName.String()}).Add(float64(n))
	})

	appendObjectResult, err := psm.Next.AppendObject(ctx, bucketName, key, reader, checksumInput, opts)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "AppendObject"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "AppendObject"}).Inc()

	return appendObjectResult, nil
}

func (psm *prometheusStorageMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.DeleteObject", "DeleteObject", func(ctx context.Context) error {
		return psm.Next.DeleteObject(ctx, bucketName, key, opts)
	})
}

func (psm *prometheusStorageMiddleware) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	var result *storage.DeleteObjectsResult
	err := psm.run(ctx, "PrometheusStorageMiddleware.DeleteObjects", "DeleteObjects", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.DeleteObjects(ctx, bucketName, entries)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	var result *storage.InitiateMultipartUploadResult
	err := psm.run(ctx, "PrometheusStorageMiddleware.CreateMultipartUpload", "CreateMultipartUpload", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType, opts)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	ctx, span := psm.tracer.Start(ctx, "PrometheusStorageMiddleware.CopyObject")
	defer span.End()

	result, err := psm.Next.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, opts)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "CopyObject"}).Inc()
		return nil, err
	}
	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "CopyObject"}).Inc()
	return result, nil
}

func (psm *prometheusStorageMiddleware) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	ctx, span := psm.tracer.Start(ctx, "PrometheusStorageMiddleware.UploadPartCopy")
	defer span.End()

	result, err := psm.Next.UploadPartCopy(ctx, srcBucket, srcKey, dstBucket, dstKey, uploadId, partNumber, opts)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "UploadPartCopy"}).Inc()
		return nil, err
	}
	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "UploadPartCopy"}).Inc()
	return result, nil
}

func (psm *prometheusStorageMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := psm.tracer.Start(ctx, "PrometheusStorageMiddleware.UploadPart")
	defer span.End()

	bytesUploaded := 0
	data = ioutils.NewStatsReadCloser(ioutils.NewNopSeekCloser(data), func(n int) {
		bytesUploaded += n
	})

	uploadPartResult, err := psm.Next.UploadPart(ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
	if err != nil {
		psm.failedApiOpsCounter.With(prometheus.Labels{"type": "UploadPart"}).Inc()
		return nil, err
	}

	psm.successfulApiOpsCounter.With(prometheus.Labels{"type": "UploadPart"}).Inc()
	psm.totalBytesUploadedByBucket.With(prometheus.Labels{"bucket": bucketName.String()}).Add(float64(bytesUploaded))

	return uploadPartResult, nil
}

func (psm *prometheusStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	var result *storage.CompleteMultipartUploadResult
	err := psm.run(ctx, "PrometheusStorageMiddleware.CompleteMultipartUpload", "CompleteMultipartUpload", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.AbortMultipartUpload", "AbortMultipartUpload", func(ctx context.Context) error {
		return psm.Next.AbortMultipartUpload(ctx, bucketName, key, uploadId)
	})
}

func (psm *prometheusStorageMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	var result *storage.ListMultipartUploadsResult
	err := psm.run(ctx, "PrometheusStorageMiddleware.ListMultipartUploads", "ListMultipartUploads", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.ListMultipartUploads(ctx, bucketName, opts)
		return err
	})
	return result, err
}

func (psm *prometheusStorageMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	var result *storage.ListPartsResult
	err := psm.run(ctx, "PrometheusStorageMiddleware.ListParts", "ListParts", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.ListParts(ctx, bucketName, key, uploadId, opts)
		return err
	})
	return result, err
}
