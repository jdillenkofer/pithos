package replication

import (
	"context"
	"errors"
	"github.com/jdillenkofer/pithos/internal/storage"
	"io"
)

func (rs *replicationStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName, options ...storage.CreateBucketOptions) error {
	_, err := rs.execute(ctx, "CreateBucket", operationPayload{Bucket: bucketName.String(), Create: options}, nil)
	return err
}
func (rs *replicationStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	_, err := rs.execute(ctx, "DeleteBucket", operationPayload{Bucket: bucketName.String()}, nil)
	return err
}
func (rs *replicationStorage) PutBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketVersioningConfiguration) error {
	_, err := rs.execute(ctx, "PutBucketVersioningConfiguration", operationPayload{Bucket: bucketName.String(), Versioning: config}, nil)
	return err
}
func (rs *replicationStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	result, err := rs.execute(ctx, "PutObject", operationPayload{Bucket: bucketName.String(), Key: key.String(), ContentType: contentType, Checksum: checksumInput, Put: opts}, reader)
	if err != nil {
		return nil, err
	}
	return result.Put, nil
}
func (rs *replicationStorage) PutObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, tags map[string]string, opts *storage.ObjectTaggingOptions) error {
	_, err := rs.execute(ctx, "PutObjectTagging", operationPayload{Bucket: bucketName.String(), Key: key.String(), Tags: tags, TagOptions: opts}, nil)
	return err
}
func (rs *replicationStorage) DeleteObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) error {
	_, err := rs.execute(ctx, "DeleteObjectTagging", operationPayload{Bucket: bucketName.String(), Key: key.String(), TagOptions: opts}, nil)
	return err
}
func (rs *replicationStorage) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	result, err := rs.execute(ctx, "AppendObject", operationPayload{Bucket: bucketName.String(), Key: key.String(), Checksum: checksumInput, Append: opts}, reader)
	if err != nil {
		return nil, err
	}
	return result.Append, nil
}
func (rs *replicationStorage) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	result, err := rs.execute(ctx, "CopyObject", operationPayload{Bucket: dstBucket.String(), Key: dstKey.String(), SourceBucket: srcBucket.String(), SourceKey: srcKey.String(), Copy: opts}, nil)
	if err != nil {
		return nil, err
	}
	return result.Copy, nil
}
func (rs *replicationStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	result, err := rs.execute(ctx, "DeleteObject", operationPayload{Bucket: bucketName.String(), Key: key.String(), Delete: opts}, nil)
	if err != nil {
		return nil, err
	}
	return result.Delete, nil
}
func (rs *replicationStorage) TransitionObjectStorageClass(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, targetStorageClass string, opts *storage.TransitionObjectStorageClassOptions) error {
	_, err := rs.execute(ctx, "TransitionObjectStorageClass", operationPayload{Bucket: bucketName.String(), Key: key.String(), StorageClass: targetStorageClass, Transition: opts}, nil)
	return err
}
func (rs *replicationStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	result, err := rs.execute(ctx, "CreateMultipartUpload", operationPayload{Bucket: bucketName.String(), Key: key.String(), ContentType: contentType, ChecksumType: checksumType, Multipart: opts}, nil)
	if err != nil {
		return nil, err
	}
	return &storage.InitiateMultipartUploadResult{UploadId: storage.MustNewUploadId(result.UploadID)}, nil
}
func (rs *replicationStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	result, err := rs.execute(ctx, "UploadPart", operationPayload{Bucket: bucketName.String(), Key: key.String(), UploadID: uploadId.String(), PartNumber: partNumber, Checksum: checksumInput}, reader)
	if err != nil {
		return nil, err
	}
	return result.Part, nil
}
func (rs *replicationStorage) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	result, err := rs.execute(ctx, "UploadPartCopy", operationPayload{Bucket: dstBucket.String(), Key: dstKey.String(), SourceBucket: srcBucket.String(), SourceKey: srcKey.String(), UploadID: uploadId.String(), PartNumber: partNumber, PartCopy: opts}, nil)
	if err != nil {
		return nil, err
	}
	return result.PartCopy, nil
}
func (rs *replicationStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	result, err := rs.execute(ctx, "CompleteMultipartUpload", operationPayload{Bucket: bucketName.String(), Key: key.String(), UploadID: uploadId.String(), Checksum: checksumInput, Complete: opts}, nil)
	if err != nil {
		return nil, err
	}
	return result.Complete, nil
}
func (rs *replicationStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	_, err := rs.execute(ctx, "AbortMultipartUpload", operationPayload{Bucket: bucketName.String(), Key: key.String(), UploadID: uploadId.String()}, nil)
	return err
}
func (rs *replicationStorage) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	_, err := rs.execute(ctx, "PutBucketWebsiteConfiguration", operationPayload{Bucket: bucketName.String(), Website: config}, nil)
	return err
}
func (rs *replicationStorage) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	_, err := rs.execute(ctx, "DeleteBucketWebsiteConfiguration", operationPayload{Bucket: bucketName.String()}, nil)
	return err
}
func (rs *replicationStorage) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	_, err := rs.execute(ctx, "PutBucketCORSConfiguration", operationPayload{Bucket: bucketName.String(), CORS: config}, nil)
	return err
}
func (rs *replicationStorage) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	_, err := rs.execute(ctx, "DeleteBucketCORSConfiguration", operationPayload{Bucket: bucketName.String()}, nil)
	return err
}
func (rs *replicationStorage) PutBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketLifecycleConfiguration) error {
	_, err := rs.execute(ctx, "PutBucketLifecycleConfiguration", operationPayload{Bucket: bucketName.String(), Lifecycle: config}, nil)
	return err
}
func (rs *replicationStorage) DeleteBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	_, err := rs.execute(ctx, "DeleteBucketLifecycleConfiguration", operationPayload{Bucket: bucketName.String()}, nil)
	return err
}
func (rs *replicationStorage) PutObjectLockConfiguration(ctx context.Context, bucket storage.BucketName, config *storage.ObjectLockConfiguration) error {
	_, err := rs.execute(ctx, "PutObjectLockConfiguration", operationPayload{Bucket: bucket.String(), LockConfiguration: config}, nil)
	return err
}
func (rs *replicationStorage) PutBucketNotificationConfiguration(ctx context.Context, bucket storage.BucketName, config *storage.BucketNotificationConfiguration) error {
	_, err := rs.execute(ctx, "PutBucketNotificationConfiguration", operationPayload{Bucket: bucket.String(), Notification: config}, nil)
	return err
}

func (rs *replicationStorage) PutObjectRetention(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, retention *storage.ObjectRetention, opts *storage.ObjectLockOptions) error {
	_, err := rs.execute(ctx, "PutObjectRetention", operationPayload{Bucket: bucket.String(), Key: key.String(), Retention: retention, LockOptions: opts}, nil)
	return err
}
func (rs *replicationStorage) PutObjectLegalHold(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, status storage.LegalHoldStatus, opts *storage.ObjectLockOptions) error {
	_, err := rs.execute(ctx, "PutObjectLegalHold", operationPayload{Bucket: bucket.String(), Key: key.String(), Hold: status, LockOptions: opts}, nil)
	return err
}
func (rs *replicationStorage) DeleteObjects(ctx context.Context, bucket storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	result := &storage.DeleteObjectsResult{}
	for _, entry := range entries {
		deleted, err := rs.DeleteObject(ctx, bucket, entry.Key, &storage.DeleteObjectOptions{VersionID: entry.VersionID, IfMatchETag: entry.IfMatchETag, BypassGovernanceRetention: entry.BypassGovernanceRetention})
		if errors.Is(err, storage.ErrNoSuchBucket) {
			return nil, err
		}
		row := storage.DeleteObjectsEntry{Key: entry.Key, VersionID: entry.VersionID}
		if err != nil {
			row.ErrCode = "InternalError"
			row.ErrMsg = err.Error()
			if errors.Is(err, storage.ErrObjectLockAccessDenied) {
				row.ErrCode = "AccessDenied"
			}
			if errors.Is(err, storage.ErrPreconditionFailed) {
				row.ErrCode = "PreconditionFailed"
			}
		} else {
			row.Deleted = true
			row.DeleteMarker = &deleted.IsDeleteMarker
			if deleted.IsDeleteMarker && entry.VersionID == nil {
				row.DeleteMarkerVersionID = deleted.VersionID
			}
		}
		result.Entries = append(result.Entries, row)
	}
	return result, nil
}
