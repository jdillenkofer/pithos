package delegator

import (
	"context"
	"github.com/jdillenkofer/pithos/internal/storage"
)

func (d *DelegatingStorage) Synchronize(ctx context.Context, bucket storage.BucketName) error {
	if next, ok := d.Next.(interface {
		Synchronize(context.Context, storage.BucketName) error
	}); ok {
		return next.Synchronize(ctx, bucket)
	}
	return nil
}

func (d *DelegatingStorage) GetObjectLockConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.ObjectLockConfiguration, error) {
	return d.Next.GetObjectLockConfiguration(ctx, bucketName)
}
func (d *DelegatingStorage) PutObjectLockConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.ObjectLockConfiguration) error {
	return d.Next.PutObjectLockConfiguration(ctx, bucketName, config)
}
func (d *DelegatingStorage) GetObjectRetention(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.ObjectRetention, error) {
	return d.Next.GetObjectRetention(ctx, bucketName, key, opts)
}
func (d *DelegatingStorage) PutObjectRetention(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, retention *storage.ObjectRetention, opts *storage.ObjectLockOptions) error {
	return d.Next.PutObjectRetention(ctx, bucketName, key, retention, opts)
}
func (d *DelegatingStorage) GetObjectLegalHold(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.LegalHoldStatus, error) {
	return d.Next.GetObjectLegalHold(ctx, bucketName, key, opts)
}
func (d *DelegatingStorage) PutObjectLegalHold(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, status storage.LegalHoldStatus, opts *storage.ObjectLockOptions) error {
	return d.Next.PutObjectLegalHold(ctx, bucketName, key, status, opts)
}
