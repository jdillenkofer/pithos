package replication

import (
	"context"
	"io"
	"sync"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type replicationStorage struct {
	*lifecycle.ValidatedLifecycle
	primaryStorage                      storage.Storage
	secondaryStorages                   []storage.Storage
	primaryUploadIdToSecondaryUploadIds map[storage.UploadId][]storage.UploadId
	mapMutex                            sync.Mutex
	tracer                              trace.Tracer
}

// Compile-time check to ensure replicationStorage implements storage.Storage
var _ storage.Storage = (*replicationStorage)(nil)

func NewStorage(primaryStorage storage.Storage, secondaryStorages ...storage.Storage) (storage.Storage, error) {
	primaryUploadIdToSecondaryUploadIds := make(map[storage.UploadId][]storage.UploadId)

	lifecycle, err := lifecycle.NewValidatedLifecycle("ReplicationStorage")
	if err != nil {
		return nil, err
	}

	return &replicationStorage{
		ValidatedLifecycle:                  lifecycle,
		primaryStorage:                      primaryStorage,
		secondaryStorages:                   secondaryStorages,
		primaryUploadIdToSecondaryUploadIds: primaryUploadIdToSecondaryUploadIds,
		mapMutex:                            sync.Mutex{},
		tracer:                              otel.Tracer("internal/storage/replication"),
	}, nil
}

func (rs *replicationStorage) Start(ctx context.Context) error {
	if err := rs.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	if err := rs.primaryStorage.Start(ctx); err != nil {
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
	if err := rs.primaryStorage.Stop(ctx); err != nil {
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

	err := rs.primaryStorage.CreateBucket(ctx, bucketName)
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

	err := rs.primaryStorage.DeleteBucket(ctx, bucketName)
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

func (rs *replicationStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.ListBuckets")
	defer span.End()

	return rs.primaryStorage.ListBuckets(ctx)
}

func (rs *replicationStorage) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.HeadBucket")
	defer span.End()

	return rs.primaryStorage.HeadBucket(ctx, bucketName)
}

func (rs *replicationStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.ListObjects")
	defer span.End()

	return rs.primaryStorage.ListObjects(ctx, bucketName, opts)
}

func (rs *replicationStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (*storage.Object, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.HeadObject")
	defer span.End()

	return rs.primaryStorage.HeadObject(ctx, bucketName, key)
}

func (rs *replicationStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.GetObject")
	defer span.End()

	return rs.primaryStorage.GetObject(ctx, bucketName, key, startByte, endByte)
}

func (rs *replicationStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.PutObject")
	defer span.End()

	// @TODO: cache reader on disk
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(data)

	putObjectResult, err := rs.primaryStorage.PutObject(ctx, bucketName, key, contentType, byteReadSeekCloser, checksumInput)
	if err != nil {
		return nil, err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		_, err = byteReadSeekCloser.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = secondaryStorage.PutObject(ctx, bucketName, key, contentType, byteReadSeekCloser, checksumInput)
		if err != nil {
			return nil, err
		}
	}
	return putObjectResult, nil
}

func (rs *replicationStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.DeleteObject")
	defer span.End()

	err := rs.primaryStorage.DeleteObject(ctx, bucketName, key)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.DeleteObject(ctx, bucketName, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.CreateMultipartUpload")
	defer span.End()

	initiateMultipartUploadResult, err := rs.primaryStorage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
	if err != nil {
		return nil, err
	}
	var secondaryUploadIds []storage.UploadId = []storage.UploadId{}
	for _, secondaryStorage := range rs.secondaryStorages {
		initiateMultipartUploadResult, err := secondaryStorage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
		if err != nil {
			return nil, err
		}
		secondaryUploadIds = append(secondaryUploadIds, initiateMultipartUploadResult.UploadId)
	}

	rs.mapMutex.Lock()
	rs.primaryUploadIdToSecondaryUploadIds[initiateMultipartUploadResult.UploadId] = secondaryUploadIds
	rs.mapMutex.Unlock()

	return initiateMultipartUploadResult, nil
}

func (rs *replicationStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.UploadPart")
	defer span.End()

	// @TODO: cache reader on disk
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(data)

	uploadPartResult, err := rs.primaryStorage.UploadPart(ctx, bucketName, key, uploadId, partNumber, byteReadSeekCloser, checksumInput)
	if err != nil {
		return nil, err
	}

	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	rs.mapMutex.Unlock()

	for i, secondaryStorage := range rs.secondaryStorages {
		_, err = byteReadSeekCloser.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = secondaryStorage.UploadPart(ctx, bucketName, key, secondaryUploadIds[i], partNumber, byteReadSeekCloser, checksumInput)
		if err != nil {
			return nil, err
		}
	}
	return uploadPartResult, nil
}

func (rs *replicationStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.CompleteMultipartUpload")
	defer span.End()

	completeMultipartUploadResult, err := rs.primaryStorage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput)
	if err != nil {
		return nil, err
	}
	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	for i, secondaryStorage := range rs.secondaryStorages {
		_, err := secondaryStorage.CompleteMultipartUpload(ctx, bucketName, key, secondaryUploadIds[i], checksumInput)
		if err != nil {
			return nil, err
		}
	}
	delete(rs.primaryUploadIdToSecondaryUploadIds, uploadId)
	rs.mapMutex.Unlock()
	return completeMultipartUploadResult, nil
}

func (rs *replicationStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.AbortMultipartUpload")
	defer span.End()

	err := rs.primaryStorage.AbortMultipartUpload(ctx, bucketName, key, uploadId)
	if err != nil {
		return err
	}
	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	for i, secondaryStorage := range rs.secondaryStorages {
		err := secondaryStorage.AbortMultipartUpload(ctx, bucketName, key, secondaryUploadIds[i])
		if err != nil {
			return err
		}
	}
	delete(rs.primaryUploadIdToSecondaryUploadIds, uploadId)
	rs.mapMutex.Unlock()
	return nil
}

func (rs *replicationStorage) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.ListMultipartUploads")
	defer span.End()

	return rs.primaryStorage.ListMultipartUploads(ctx, bucketName, opts)
}

func (rs *replicationStorage) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	ctx, span := rs.tracer.Start(ctx, "ReplicationStorage.ListParts")
	defer span.End()

	return rs.primaryStorage.ListParts(ctx, bucketName, key, uploadId, opts)
}
