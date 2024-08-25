package storage

import (
	"context"
	"io"

	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusStorageMiddleware struct {
	registerer              prometheus.Registerer
	failedApiOpsCounter     *prometheus.CounterVec
	successfulApiOpsCounter *prometheus.CounterVec
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

	return &PrometheusStorageMiddleware{
		registerer:              registerer,
		failedApiOpsCounter:     failedApiOpsCounter,
		successfulApiOpsCounter: successfulApiOpsCounter,
		innerStorage:            innerStorage,
	}, nil
}

func (psm *PrometheusStorageMiddleware) Start(ctx context.Context) error {
	psm.registerer.MustRegister(psm.failedApiOpsCounter)
	psm.registerer.MustRegister(psm.successfulApiOpsCounter)
	err := psm.innerStorage.Start(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (psm *PrometheusStorageMiddleware) Stop(ctx context.Context) error {
	psm.registerer.Unregister(psm.successfulApiOpsCounter)
	psm.registerer.Unregister(psm.failedApiOpsCounter)
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
