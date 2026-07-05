package replication

import (
	"context"
	"database/sql"
	"io"
	"sync"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const maxMemoryCacheSize = 10 * 1000 * 1000

type replicationStorage struct {
	*lifecycle.ValidatedLifecycle
	delegator.DelegatingStorage
	secondaryStorages                   []storage.Storage
	primaryUploadIdToSecondaryUploadIds map[storage.UploadId][]storage.UploadId
	mapMutex                            sync.Mutex
	tracer                              trace.Tracer
}

var _ storage.Storage = (*replicationStorage)(nil)
var _ storage.TransactionalStorage = (*replicationStorage)(nil)

func NewStorage(primaryStorage storage.Storage, secondaryStorages ...storage.Storage) (storage.Storage, error) {
	lc, err := lifecycle.NewValidatedLifecycle("ReplicationStorage")
	if err != nil {
		return nil, err
	}

	return &replicationStorage{
		ValidatedLifecycle:                  lc,
		DelegatingStorage:                   delegator.Wrap(primaryStorage),
		secondaryStorages:                   secondaryStorages,
		primaryUploadIdToSecondaryUploadIds: make(map[storage.UploadId][]storage.UploadId),
		mapMutex:                            sync.Mutex{},
		tracer:                              otel.Tracer("internal/storage/replication"),
	}, nil
}

func (rs *replicationStorage) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	txPrimary, ok := rs.Next.(storage.TransactionalStorage)
	if !ok {
		return fn(ctx, rs)
	}
	return txPrimary.WithTransaction(ctx, opts, func(ctx context.Context, _ storage.Storage) error {
		return fn(ctx, rs)
	})
}

func (rs *replicationStorage) Start(ctx context.Context) error {
	if err := rs.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	if err := rs.Next.Start(ctx); err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		if err := secondaryStorage.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) Stop(ctx context.Context) error {
	if err := rs.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	if err := rs.Next.Stop(ctx); err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		if err := secondaryStorage.Stop(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.CreateBucket")
	defer span.End()

	err := rs.Next.CreateBucket(ctx, bucketName)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.CreateBucket(ctx, bucketName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.DeleteBucket")
	defer span.End()

	err := rs.Next.DeleteBucket(ctx, bucketName)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.DeleteBucket(ctx, bucketName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) PutBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketVersioningConfiguration) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.PutBucketVersioningConfiguration")
	defer span.End()

	if err := rs.Next.PutBucketVersioningConfiguration(ctx, bucketName, config); err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		if err := secondaryStorage.PutBucketVersioningConfiguration(ctx, bucketName, config); err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.PutObject")
	defer span.End()

	readSeekCloser, err := ioutils.NewSmartCachedReadSeekCloser(reader, maxMemoryCacheSize)
	if err != nil {
		return nil, err
	}
	defer readSeekCloser.Close()

	putObjectResult, err := rs.Next.PutObject(ctx, bucketName, key, contentType, readSeekCloser, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	// Secondaries must not re-evaluate conditional-write preconditions (the
	// primary already enforced them), but the tag set, object metadata and
	// storage class apply to every replica.
	var secondaryOpts *storage.PutObjectOptions
	if opts != nil && (len(opts.Tags) > 0 || opts.Metadata != nil || opts.StorageClass != nil) {
		secondaryOpts = &storage.PutObjectOptions{Tags: opts.Tags, Metadata: opts.Metadata, StorageClass: opts.StorageClass}
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		_, err = readSeekCloser.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = secondaryStorage.PutObject(ctx, bucketName, key, contentType, readSeekCloser, checksumInput, secondaryOpts)
		if err != nil {
			return nil, err
		}
	}
	return putObjectResult, nil
}

func (rs *replicationStorage) PutObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, tags map[string]string) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.PutObjectTagging")
	defer span.End()

	err := rs.Next.PutObjectTagging(ctx, bucketName, key, tags)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.PutObjectTagging(ctx, bucketName, key, tags)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) DeleteObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.DeleteObjectTagging")
	defer span.End()

	err := rs.Next.DeleteObjectTagging(ctx, bucketName, key)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.DeleteObjectTagging(ctx, bucketName, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.AppendObject")
	defer span.End()

	readSeekCloser, err := ioutils.NewSmartCachedReadSeekCloser(reader, maxMemoryCacheSize)
	if err != nil {
		return nil, err
	}
	defer readSeekCloser.Close()

	appendObjectResult, err := rs.Next.AppendObject(ctx, bucketName, key, readSeekCloser, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		_, err = readSeekCloser.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = secondaryStorage.AppendObject(ctx, bucketName, key, readSeekCloser, checksumInput, nil)
		if err != nil {
			return nil, err
		}
	}
	return appendObjectResult, nil
}

func (rs *replicationStorage) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.CopyObject")
	defer span.End()

	result, err := rs.Next.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, opts)
	if err != nil {
		return nil, err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		if _, err = secondaryStorage.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, opts); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (rs *replicationStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.DeleteObject")
	defer span.End()

	result, err := rs.Next.DeleteObject(ctx, bucketName, key, opts)
	if err != nil {
		return nil, err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		_, err = secondaryStorage.DeleteObject(ctx, bucketName, key, opts)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (rs *replicationStorage) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.DeleteObjects")
	defer span.End()

	result, err := rs.Next.DeleteObjects(ctx, bucketName, entries)
	if err != nil {
		return nil, err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		_, err = secondaryStorage.DeleteObjects(ctx, bucketName, entries)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (rs *replicationStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.CreateMultipartUpload")
	defer span.End()

	primaryResult, err := rs.Next.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType, opts)
	if err != nil {
		return nil, err
	}
	secondaryUploadIDs := make([]storage.UploadId, 0, len(rs.secondaryStorages))
	for _, secondaryStorage := range rs.secondaryStorages {
		secondaryResult, err := secondaryStorage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType, opts)
		if err != nil {
			return nil, err
		}
		secondaryUploadIDs = append(secondaryUploadIDs, secondaryResult.UploadId)
	}

	rs.mapMutex.Lock()
	rs.primaryUploadIdToSecondaryUploadIds[primaryResult.UploadId] = secondaryUploadIDs
	rs.mapMutex.Unlock()

	return primaryResult, nil
}

func (rs *replicationStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.UploadPart")
	defer span.End()

	readSeekCloser, err := ioutils.NewSmartCachedReadSeekCloser(reader, maxMemoryCacheSize)
	if err != nil {
		return nil, err
	}
	defer readSeekCloser.Close()

	uploadPartResult, err := rs.Next.UploadPart(ctx, bucketName, key, uploadId, partNumber, readSeekCloser, checksumInput)
	if err != nil {
		return nil, err
	}

	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	rs.mapMutex.Unlock()

	for i, secondaryStorage := range rs.secondaryStorages {
		_, err = readSeekCloser.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = secondaryStorage.UploadPart(ctx, bucketName, key, secondaryUploadIds[i], partNumber, readSeekCloser, checksumInput)
		if err != nil {
			return nil, err
		}
	}
	return uploadPartResult, nil
}

func (rs *replicationStorage) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.UploadPartCopy")
	defer span.End()

	uploadPartCopyResult, err := rs.Next.UploadPartCopy(ctx, srcBucket, srcKey, dstBucket, dstKey, uploadId, partNumber, opts)
	if err != nil {
		return nil, err
	}

	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	rs.mapMutex.Unlock()

	for i, secondaryStorage := range rs.secondaryStorages {
		if _, err = secondaryStorage.UploadPartCopy(ctx, srcBucket, srcKey, dstBucket, dstKey, secondaryUploadIds[i], partNumber, opts); err != nil {
			return nil, err
		}
	}
	return uploadPartCopyResult, nil
}

func completeMultipartUploadPartsOnlyOptions(opts *storage.CompleteMultipartUploadOptions) *storage.CompleteMultipartUploadOptions {
	if opts == nil || len(opts.Parts) == 0 {
		return nil
	}
	return &storage.CompleteMultipartUploadOptions{
		Parts: opts.Parts,
	}
}

func (rs *replicationStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.CompleteMultipartUpload")
	defer span.End()

	completeMultipartUploadResult, err := rs.Next.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
	if err != nil {
		return nil, err
	}

	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	rs.mapMutex.Unlock()

	secondaryOpts := completeMultipartUploadPartsOnlyOptions(opts)
	for i, secondaryStorage := range rs.secondaryStorages {
		_, err := secondaryStorage.CompleteMultipartUpload(ctx, bucketName, key, secondaryUploadIds[i], checksumInput, secondaryOpts)
		if err != nil {
			return nil, err
		}
	}

	rs.mapMutex.Lock()
	delete(rs.primaryUploadIdToSecondaryUploadIds, uploadId)
	rs.mapMutex.Unlock()
	return completeMultipartUploadResult, nil
}

func (rs *replicationStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.AbortMultipartUpload")
	defer span.End()

	err := rs.Next.AbortMultipartUpload(ctx, bucketName, key, uploadId)
	if err != nil {
		return err
	}

	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	rs.mapMutex.Unlock()

	for i, secondaryStorage := range rs.secondaryStorages {
		err := secondaryStorage.AbortMultipartUpload(ctx, bucketName, key, secondaryUploadIds[i])
		if err != nil {
			return err
		}
	}

	rs.mapMutex.Lock()
	delete(rs.primaryUploadIdToSecondaryUploadIds, uploadId)
	rs.mapMutex.Unlock()
	return nil
}

func (rs *replicationStorage) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.PutBucketWebsiteConfiguration")
	defer span.End()

	err := rs.Next.PutBucketWebsiteConfiguration(ctx, bucketName, config)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.PutBucketWebsiteConfiguration(ctx, bucketName, config)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.DeleteBucketWebsiteConfiguration")
	defer span.End()

	err := rs.Next.DeleteBucketWebsiteConfiguration(ctx, bucketName)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.DeleteBucketWebsiteConfiguration(ctx, bucketName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) GetBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.GetBucketCORSConfiguration")
	defer span.End()

	return rs.Next.GetBucketCORSConfiguration(ctx, bucketName)
}

func (rs *replicationStorage) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.PutBucketCORSConfiguration")
	defer span.End()

	err := rs.Next.PutBucketCORSConfiguration(ctx, bucketName, config)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.PutBucketCORSConfiguration(ctx, bucketName, config)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.DeleteBucketCORSConfiguration")
	defer span.End()

	err := rs.Next.DeleteBucketCORSConfiguration(ctx, bucketName)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.DeleteBucketCORSConfiguration(ctx, bucketName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) GetBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketLifecycleConfiguration, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.GetBucketLifecycleConfiguration")
	defer span.End()

	return rs.Next.GetBucketLifecycleConfiguration(ctx, bucketName)
}

func (rs *replicationStorage) PutBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketLifecycleConfiguration) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.PutBucketLifecycleConfiguration")
	defer span.End()

	err := rs.Next.PutBucketLifecycleConfiguration(ctx, bucketName, config)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.PutBucketLifecycleConfiguration(ctx, bucketName, config)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) DeleteBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.DeleteBucketLifecycleConfiguration")
	defer span.End()

	err := rs.Next.DeleteBucketLifecycleConfiguration(ctx, bucketName)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.DeleteBucketLifecycleConfiguration(ctx, bucketName)
		if err != nil {
			return err
		}
	}
	return nil
}
