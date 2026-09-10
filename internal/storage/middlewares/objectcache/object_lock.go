package objectcache

import (
	"context"
	"github.com/jdillenkofer/pithos/internal/storage"
)

func (m *objectCacheStorageMiddleware) PutObjectRetention(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, retention *storage.ObjectRetention, opts *storage.ObjectLockOptions) error {
	err := m.Next.PutObjectRetention(ctx, bucket, key, retention, opts)
	m.invalidateObjectCaches(ctx, bucket, key)
	return err
}
func (m *objectCacheStorageMiddleware) PutObjectLegalHold(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, hold storage.LegalHoldStatus, opts *storage.ObjectLockOptions) error {
	err := m.Next.PutObjectLegalHold(ctx, bucket, key, hold, opts)
	m.invalidateObjectCaches(ctx, bucket, key)
	return err
}
