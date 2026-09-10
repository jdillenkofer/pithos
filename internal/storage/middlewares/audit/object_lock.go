package audit

import (
	"context"
	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/storage"
	"time"
)

func lockValues(lock storage.ObjectLock) auditlog.ObjectLockValues {
	values := auditlog.ObjectLockValues{}
	if r := lock.Retention; r != nil {
		values.Mode = string(r.Mode)
		values.RetainUntilDate = r.RetainUntilDate.UTC().Format(time.RFC3339Nano)
	}
	if lock.LegalHold != nil {
		values.LegalHold = string(*lock.LegalHold)
	}
	return values
}
func configurationValues(config *storage.ObjectLockConfiguration) auditlog.ObjectLockValues {
	values := auditlog.ObjectLockValues{}
	if config != nil {
		values.Enabled = config.ObjectLockEnabled
		if d := config.DefaultRetention; d != nil {
			values.Mode = string(d.Mode)
			values.Days = d.Days
			values.Years = d.Years
		}
	}
	return values
}
func lockDetails(lock storage.ObjectLock, bypass bool) *auditlog.ObjectLockDetails {
	return &auditlog.ObjectLockDetails{Requested: lockValues(lock), BypassRequested: bypass, BypassAuthorized: bypass}
}

func (m *AuditLogMiddleware) GetObjectLockConfiguration(ctx context.Context, bucket storage.BucketName) (*storage.ObjectLockConfiguration, error) {
	var result *storage.ObjectLockConfiguration
	err := m.run(ctx, auditlog.OpGetObjectLockConfiguration, auditResource{bucket: bucket.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.GetObjectLockConfiguration(ctx, bucket)
		return err
	})
	return result, err
}
func (m *AuditLogMiddleware) PutObjectLockConfiguration(ctx context.Context, bucket storage.BucketName, config *storage.ObjectLockConfiguration) error {
	return m.run(ctx, auditlog.OpPutObjectLockConfiguration, auditResource{bucket: bucket.String(), objectLock: &auditlog.ObjectLockDetails{Requested: configurationValues(config)}}, func(ctx context.Context) error { return m.Next.PutObjectLockConfiguration(ctx, bucket, config) })
}
func lockResource(bucket storage.BucketName, key storage.ObjectKey, lock storage.ObjectLock, opts *storage.ObjectLockOptions) auditResource {
	r := auditResource{bucket: bucket.String(), key: key.String(), objectLock: lockDetails(lock, opts != nil && opts.BypassGovernanceRetention)}
	if opts != nil {
		r.versionID = opts.VersionID
	}
	return r
}
func (m *AuditLogMiddleware) GetObjectRetention(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.ObjectRetention, error) {
	var result *storage.ObjectRetention
	err := m.run(ctx, auditlog.OpGetObjectRetention, lockResource(bucket, key, storage.ObjectLock{}, opts), func(ctx context.Context) error {
		var err error
		result, err = m.Next.GetObjectRetention(ctx, bucket, key, opts)
		return err
	})
	return result, err
}
func (m *AuditLogMiddleware) PutObjectRetention(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, retention *storage.ObjectRetention, opts *storage.ObjectLockOptions) error {
	return m.run(ctx, auditlog.OpPutObjectRetention, lockResource(bucket, key, storage.ObjectLock{Retention: retention}, opts), func(ctx context.Context) error { return m.Next.PutObjectRetention(ctx, bucket, key, retention, opts) })
}
func (m *AuditLogMiddleware) GetObjectLegalHold(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.LegalHoldStatus, error) {
	var result *storage.LegalHoldStatus
	err := m.run(ctx, auditlog.OpGetObjectLegalHold, lockResource(bucket, key, storage.ObjectLock{}, opts), func(ctx context.Context) error {
		var err error
		result, err = m.Next.GetObjectLegalHold(ctx, bucket, key, opts)
		return err
	})
	return result, err
}
func (m *AuditLogMiddleware) PutObjectLegalHold(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, status storage.LegalHoldStatus, opts *storage.ObjectLockOptions) error {
	return m.run(ctx, auditlog.OpPutObjectLegalHold, lockResource(bucket, key, storage.ObjectLock{LegalHold: &status}, opts), func(ctx context.Context) error { return m.Next.PutObjectLegalHold(ctx, bucket, key, status, opts) })
}

// RecordAuthorizationDenied feeds HTTP denials into the same hash chain. It is
// only called before storage dispatch, so successful calls are not duplicated.
func (m *AuditLogMiddleware) RecordAuthorizationDenied(ctx context.Context, operation auditlog.Operation, resource auditlog.ResourceDetails, lock *auditlog.ObjectLockDetails) {
	m.log(ctx, operation, auditlog.PhaseComplete, auditResource{bucket: resource.Bucket, key: resource.Key, versionID: &resource.VersionID, objectLock: lock, denied: true}, storage.ErrObjectLockAccessDenied, 403, 0)
}
