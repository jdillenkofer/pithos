package s3client

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/storage"
	"time"
)

func objectLockError(err error) error {
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "ObjectLockConfigurationNotFoundError":
			return storage.ErrObjectLockConfigurationNotFound
		case "NoSuchBucket":
			return storage.ErrNoSuchBucket
		case "NoSuchKey", "NoSuchVersion":
			return storage.ErrNoSuchKey
		case "AccessDenied":
			return storage.ErrObjectLockAccessDenied
		case "MethodNotAllowed":
			return storage.ErrObjectLockMethodNotAllowed
		}
	}
	return err
}

func (rs *s3ClientStorage) GetObjectLockConfiguration(ctx context.Context, bucket storage.BucketName) (*storage.ObjectLockConfiguration, error) {
	out, err := rs.s3Client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String(bucket.String())})
	if err != nil {
		return nil, objectLockError(err)
	}
	if out.ObjectLockConfiguration == nil {
		return nil, storage.ErrObjectLockConfigurationNotFound
	}
	c := out.ObjectLockConfiguration
	result := &storage.ObjectLockConfiguration{ObjectLockEnabled: string(c.ObjectLockEnabled)}
	if c.Rule != nil && c.Rule.DefaultRetention != nil {
		d := c.Rule.DefaultRetention
		result.DefaultRetention = &storage.DefaultRetention{Mode: storage.RetentionMode(d.Mode), Days: d.Days, Years: d.Years}
	}
	return result, nil
}

func (rs *s3ClientStorage) PutObjectLockConfiguration(ctx context.Context, bucket storage.BucketName, config *storage.ObjectLockConfiguration) error {
	if err := config.Validate(); err != nil {
		return err
	}
	c := &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled}
	if d := config.DefaultRetention; d != nil {
		c.Rule = &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{Mode: types.ObjectLockRetentionMode(d.Mode), Days: d.Days, Years: d.Years}}
	}
	_, err := rs.s3Client.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{Bucket: aws.String(bucket.String()), ObjectLockConfiguration: c})
	return objectLockError(err)
}

func lockVersion(opts *storage.ObjectLockOptions) *string {
	if opts == nil {
		return nil
	}
	return opts.VersionID
}

func (rs *s3ClientStorage) GetObjectRetention(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.ObjectRetention, error) {
	out, err := rs.s3Client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: aws.String(bucket.String()), Key: aws.String(key.String()), VersionId: lockVersion(opts)})
	if err != nil {
		return nil, objectLockError(err)
	}
	if out.Retention == nil || out.Retention.RetainUntilDate == nil {
		return nil, nil
	}
	return &storage.ObjectRetention{Mode: storage.RetentionMode(out.Retention.Mode), RetainUntilDate: out.Retention.RetainUntilDate.UTC()}, nil
}

func (rs *s3ClientStorage) PutObjectRetention(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, retention *storage.ObjectRetention, opts *storage.ObjectLockOptions) error {
	r := &types.ObjectLockRetention{}
	if retention != nil {
		r.Mode = types.ObjectLockRetentionMode(retention.Mode)
		until := retention.RetainUntilDate.UTC()
		r.RetainUntilDate = &until
	}
	_, err := rs.s3Client.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{Bucket: aws.String(bucket.String()), Key: aws.String(key.String()), VersionId: lockVersion(opts), Retention: r, BypassGovernanceRetention: aws.Bool(opts != nil && opts.BypassGovernanceRetention)})
	return objectLockError(err)
}

func (rs *s3ClientStorage) GetObjectLegalHold(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.LegalHoldStatus, error) {
	out, err := rs.s3Client.GetObjectLegalHold(ctx, &s3.GetObjectLegalHoldInput{Bucket: aws.String(bucket.String()), Key: aws.String(key.String()), VersionId: lockVersion(opts)})
	if err != nil {
		return nil, objectLockError(err)
	}
	if out.LegalHold == nil || out.LegalHold.Status == "" {
		return nil, nil
	}
	status := storage.LegalHoldStatus(out.LegalHold.Status)
	return &status, nil
}

func (rs *s3ClientStorage) PutObjectLegalHold(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, status storage.LegalHoldStatus, opts *storage.ObjectLockOptions) error {
	if !status.Valid() {
		return storage.ErrInvalidObjectLockConfiguration
	}
	_, err := rs.s3Client.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: aws.String(bucket.String()), Key: aws.String(key.String()), VersionId: lockVersion(opts), LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatus(status)}})
	return objectLockError(err)
}

func lockFromAWS(mode types.ObjectLockMode, until *time.Time, hold types.ObjectLockLegalHoldStatus) storage.ObjectLock {
	lock := storage.ObjectLock{}
	if mode != "" && until != nil {
		lock.Retention = &storage.ObjectRetention{Mode: storage.RetentionMode(mode), RetainUntilDate: until.UTC()}
	}
	if hold != "" {
		status := storage.LegalHoldStatus(hold)
		lock.LegalHold = &status
	}
	return lock
}
