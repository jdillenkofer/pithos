package outbox

import (
	"context"
	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/storage"
)

// Synchronize is the acknowledgment barrier used by synchronous replication.
func (os *outboxStorage) Synchronize(ctx context.Context, bucket storage.BucketName) error {
	return os.waitForAllOutboxEntriesOfBucket(ctx, bucket)
}

func (os *outboxStorage) GetObjectLockConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.ObjectLockConfiguration, error) {
	if err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName); err != nil {
		return nil, err
	}
	return os.innerStorage.GetObjectLockConfiguration(ctx, bucketName)
}
func (os *outboxStorage) PutObjectLockConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.ObjectLockConfiguration) error {
	if err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName); err != nil {
		return err
	}
	return os.innerStorage.PutObjectLockConfiguration(ctx, bucketName, config)
}
func (os *outboxStorage) GetObjectRetention(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.ObjectRetention, error) {
	if err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName); err != nil {
		return nil, err
	}
	return os.innerStorage.GetObjectRetention(ctx, bucketName, key, opts)
}
func (os *outboxStorage) PutObjectRetention(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, retention *storage.ObjectRetention, opts *storage.ObjectLockOptions) error {
	if err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName); err != nil {
		return err
	}
	return os.innerStorage.PutObjectRetention(ctx, bucketName, key, retention, opts)
}
func (os *outboxStorage) GetObjectLegalHold(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.LegalHoldStatus, error) {
	if err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName); err != nil {
		return nil, err
	}
	return os.innerStorage.GetObjectLegalHold(ctx, bucketName, key, opts)
}
func (os *outboxStorage) PutObjectLegalHold(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, status storage.LegalHoldStatus, opts *storage.ObjectLockOptions) error {
	if err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName); err != nil {
		return err
	}
	return os.innerStorage.PutObjectLegalHold(ctx, bucketName, key, status, opts)
}

func (os *outboxStorage) Unwrap() storage.Storage { return os.innerStorage }

func (os *outboxStorage) RecordAuthorizationDenied(ctx context.Context, operation auditlog.Operation, resource auditlog.ResourceDetails, lock *auditlog.ObjectLockDetails) {
	if recorder, ok := os.innerStorage.(auditlog.AuthorizationDenialRecorder); ok {
		recorder.RecordAuthorizationDenied(ctx, operation, resource, lock)
	}
}
