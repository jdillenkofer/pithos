package prometheus

import (
	"context"
	"github.com/jdillenkofer/pithos/internal/storage"
)

func (psm *prometheusStorageMiddleware) GetObjectLockConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.ObjectLockConfiguration, error) {
	var result *storage.ObjectLockConfiguration
	err := psm.run(ctx, "PrometheusStorageMiddleware.GetObjectLockConfiguration", "GetObjectLockConfiguration", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.GetObjectLockConfiguration(ctx, bucketName)
		return err
	})
	return result, err
}
func (psm *prometheusStorageMiddleware) PutObjectLockConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.ObjectLockConfiguration) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.PutObjectLockConfiguration", "PutObjectLockConfiguration", func(ctx context.Context) error {
		return psm.Next.PutObjectLockConfiguration(ctx, bucketName, config)
	})
}
func (psm *prometheusStorageMiddleware) GetObjectRetention(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.ObjectRetention, error) {
	var result *storage.ObjectRetention
	err := psm.run(ctx, "PrometheusStorageMiddleware.GetObjectRetention", "GetObjectRetention", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.GetObjectRetention(ctx, bucketName, key, opts)
		return err
	})
	return result, err
}
func (psm *prometheusStorageMiddleware) PutObjectRetention(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, retention *storage.ObjectRetention, opts *storage.ObjectLockOptions) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.PutObjectRetention", "PutObjectRetention", func(ctx context.Context) error {
		return psm.Next.PutObjectRetention(ctx, bucketName, key, retention, opts)
	})
}
func (psm *prometheusStorageMiddleware) GetObjectLegalHold(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.LegalHoldStatus, error) {
	var result *storage.LegalHoldStatus
	err := psm.run(ctx, "PrometheusStorageMiddleware.GetObjectLegalHold", "GetObjectLegalHold", func(ctx context.Context) error {
		var err error
		result, err = psm.Next.GetObjectLegalHold(ctx, bucketName, key, opts)
		return err
	})
	return result, err
}
func (psm *prometheusStorageMiddleware) PutObjectLegalHold(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, status storage.LegalHoldStatus, opts *storage.ObjectLockOptions) error {
	return psm.run(ctx, "PrometheusStorageMiddleware.PutObjectLegalHold", "PutObjectLegalHold", func(ctx context.Context) error {
		return psm.Next.PutObjectLegalHold(ctx, bucketName, key, status, opts)
	})
}
