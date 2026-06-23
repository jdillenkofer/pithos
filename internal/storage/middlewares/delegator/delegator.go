package delegator

import (
	"context"
	"database/sql"
	"io"

	"github.com/jdillenkofer/pithos/internal/storage"
)

type DelegatingStorage struct {
	Next storage.Storage
}

func Wrap(next storage.Storage) DelegatingStorage {
	return DelegatingStorage{Next: next}
}

var _ storage.Storage = (*DelegatingStorage)(nil)
var _ storage.TransactionalStorage = (*DelegatingStorage)(nil)

func (d *DelegatingStorage) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return WithTransaction(ctx, opts, d.Next, d, fn)
}

func WithTransaction(ctx context.Context, opts *sql.TxOptions, next storage.Storage, outer storage.Storage, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	txStorage, ok := next.(storage.TransactionalStorage)
	if !ok {
		return fn(ctx, outer)
	}
	return txStorage.WithTransaction(ctx, opts, func(ctx context.Context, _ storage.Storage) error {
		return fn(ctx, outer)
	})
}

func (d *DelegatingStorage) Start(ctx context.Context) error {
	return d.Next.Start(ctx)
}

func (d *DelegatingStorage) Stop(ctx context.Context) error {
	return d.Next.Stop(ctx)
}

func (d *DelegatingStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	return d.Next.CreateBucket(ctx, bucketName)
}

func (d *DelegatingStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	return d.Next.DeleteBucket(ctx, bucketName)
}

func (d *DelegatingStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	return d.Next.ListBuckets(ctx)
}

func (d *DelegatingStorage) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	return d.Next.HeadBucket(ctx, bucketName)
}

func (d *DelegatingStorage) GetBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.WebsiteConfiguration, error) {
	return d.Next.GetBucketWebsiteConfiguration(ctx, bucketName)
}

func (d *DelegatingStorage) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	return d.Next.PutBucketWebsiteConfiguration(ctx, bucketName, config)
}

func (d *DelegatingStorage) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	return d.Next.DeleteBucketWebsiteConfiguration(ctx, bucketName)
}

func (d *DelegatingStorage) GetBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	return d.Next.GetBucketCORSConfiguration(ctx, bucketName)
}

func (d *DelegatingStorage) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	return d.Next.PutBucketCORSConfiguration(ctx, bucketName, config)
}

func (d *DelegatingStorage) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	return d.Next.DeleteBucketCORSConfiguration(ctx, bucketName)
}

func (d *DelegatingStorage) GetObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (map[string]string, error) {
	return d.Next.GetObjectTagging(ctx, bucketName, key)
}

func (d *DelegatingStorage) PutObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, tags map[string]string) error {
	return d.Next.PutObjectTagging(ctx, bucketName, key, tags)
}

func (d *DelegatingStorage) DeleteObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	return d.Next.DeleteObjectTagging(ctx, bucketName, key)
}

func (d *DelegatingStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	return d.Next.ListObjects(ctx, bucketName, opts)
}

func (d *DelegatingStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	return d.Next.HeadObject(ctx, bucketName, key, opts)
}

func (d *DelegatingStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	return d.Next.GetObject(ctx, bucketName, key, ranges, opts)
}

func (d *DelegatingStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	return d.Next.PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
}

func (d *DelegatingStorage) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	return d.Next.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, opts)
}

func (d *DelegatingStorage) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	return d.Next.AppendObject(ctx, bucketName, key, data, checksumInput, opts)
}

func (d *DelegatingStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) error {
	return d.Next.DeleteObject(ctx, bucketName, key, opts)
}

func (d *DelegatingStorage) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	return d.Next.DeleteObjects(ctx, bucketName, entries)
}

func (d *DelegatingStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, tags map[string]string) (*storage.InitiateMultipartUploadResult, error) {
	return d.Next.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType, tags)
}

func (d *DelegatingStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	return d.Next.UploadPart(ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
}

func (d *DelegatingStorage) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	return d.Next.UploadPartCopy(ctx, srcBucket, srcKey, dstBucket, dstKey, uploadId, partNumber, opts)
}

func (d *DelegatingStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	return d.Next.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
}

func (d *DelegatingStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	return d.Next.AbortMultipartUpload(ctx, bucketName, key, uploadId)
}

func (d *DelegatingStorage) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	return d.Next.ListMultipartUploads(ctx, bucketName, opts)
}

func (d *DelegatingStorage) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	return d.Next.ListParts(ctx, bucketName, key, uploadId, opts)
}
