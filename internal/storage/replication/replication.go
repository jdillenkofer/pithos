package replication

import (
	"context"
	"io"
	"sync"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/startstopvalidator"
)

type replicationStorage struct {
	primaryStorage                      storage.Storage
	secondaryStorages                   []storage.Storage
	primaryUploadIdToSecondaryUploadIds map[string][]string
	mapMutex                            sync.Mutex
	startStopValidator                  *startstopvalidator.StartStopValidator
}

func NewStorage(primaryStorage storage.Storage, secondaryStorages ...storage.Storage) (storage.Storage, error) {
	primaryUploadIdToSecondaryUploadIds := make(map[string][]string)

	startStopValidator, err := startstopvalidator.New("ReplicationStorage")
	if err != nil {
		return nil, err
	}

	return &replicationStorage{
		primaryStorage:                      primaryStorage,
		secondaryStorages:                   secondaryStorages,
		primaryUploadIdToSecondaryUploadIds: primaryUploadIdToSecondaryUploadIds,
		mapMutex:                            sync.Mutex{},
		startStopValidator:                  startStopValidator,
	}, nil
}

func (rs *replicationStorage) Start(ctx context.Context) error {
	err := rs.startStopValidator.Start()
	if err != nil {
		return err
	}
	err = rs.primaryStorage.Start(ctx)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.Start(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) Stop(ctx context.Context) error {
	err := rs.startStopValidator.Stop()
	if err != nil {
		return err
	}
	err = rs.primaryStorage.Stop(ctx)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.Stop(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) CreateBucket(ctx context.Context, bucket string) error {
	err := rs.primaryStorage.CreateBucket(ctx, bucket)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.CreateBucket(ctx, bucket)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) DeleteBucket(ctx context.Context, bucket string) error {
	err := rs.primaryStorage.DeleteBucket(ctx, bucket)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.DeleteBucket(ctx, bucket)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	return rs.primaryStorage.ListBuckets(ctx)
}

func (rs *replicationStorage) HeadBucket(ctx context.Context, bucket string) (*storage.Bucket, error) {
	return rs.primaryStorage.HeadBucket(ctx, bucket)
}

func (rs *replicationStorage) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*storage.ListBucketResult, error) {
	return rs.primaryStorage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
}

func (rs *replicationStorage) HeadObject(ctx context.Context, bucket string, key string) (*storage.Object, error) {
	return rs.primaryStorage.HeadObject(ctx, bucket, key)
}

func (rs *replicationStorage) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	return rs.primaryStorage.GetObject(ctx, bucket, key, startByte, endByte)
}

func (rs *replicationStorage) PutObject(ctx context.Context, bucket string, key string, reader io.Reader) error {
	// @TODO: cache reader on disk
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(data)

	err = rs.primaryStorage.PutObject(ctx, bucket, key, byteReadSeekCloser)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		_, err = byteReadSeekCloser.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		err = secondaryStorage.PutObject(ctx, bucket, key, byteReadSeekCloser)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) DeleteObject(ctx context.Context, bucket string, key string) error {
	err := rs.primaryStorage.DeleteObject(ctx, bucket, key)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.DeleteObject(ctx, bucket, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) CreateMultipartUpload(ctx context.Context, bucket string, key string) (*storage.InitiateMultipartUploadResult, error) {
	initiateMultipartUploadResult, err := rs.primaryStorage.CreateMultipartUpload(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	var secondaryUploadIds []string = []string{}
	for _, secondaryStorage := range rs.secondaryStorages {
		initiateMultipartUploadResult, err := secondaryStorage.CreateMultipartUpload(ctx, bucket, key)
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

func (rs *replicationStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, reader io.Reader) (*storage.UploadPartResult, error) {
	// @TODO: cache reader on disk
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(data)

	uploadPartResult, err := rs.primaryStorage.UploadPart(ctx, bucket, key, uploadId, partNumber, byteReadSeekCloser)
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
		_, err = secondaryStorage.UploadPart(ctx, bucket, key, secondaryUploadIds[i], partNumber, byteReadSeekCloser)
		if err != nil {
			return nil, err
		}
	}
	return uploadPartResult, nil
}

func (rs *replicationStorage) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*storage.CompleteMultipartUploadResult, error) {
	completeMultipartUploadResult, err := rs.primaryStorage.CompleteMultipartUpload(ctx, bucket, key, uploadId)
	if err != nil {
		return nil, err
	}
	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	for i, secondaryStorage := range rs.secondaryStorages {
		_, err := secondaryStorage.CompleteMultipartUpload(ctx, bucket, key, secondaryUploadIds[i])
		if err != nil {
			return nil, err
		}
	}
	delete(rs.primaryUploadIdToSecondaryUploadIds, uploadId)
	rs.mapMutex.Unlock()
	return completeMultipartUploadResult, nil
}

func (rs *replicationStorage) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	err := rs.primaryStorage.AbortMultipartUpload(ctx, bucket, key, uploadId)
	if err != nil {
		return err
	}
	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	for i, secondaryStorage := range rs.secondaryStorages {
		err := secondaryStorage.AbortMultipartUpload(ctx, bucket, key, secondaryUploadIds[i])
		if err != nil {
			return err
		}
	}
	delete(rs.primaryUploadIdToSecondaryUploadIds, uploadId)
	rs.mapMutex.Unlock()
	return nil
}
