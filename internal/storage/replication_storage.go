package storage

import (
	"context"
	"io"
	"sync"

	"github.com/jdillenkofer/pithos/internal/ioutils"
)

type ReplicationStorage struct {
	primaryStorage                      Storage
	secondaryStorages                   []Storage
	primaryUploadIdToSecondaryUploadIds map[string][]string
	mapMutex                            sync.Mutex
}

func NewReplicationStorage(primaryStorage Storage, secondaryStorages ...Storage) (*ReplicationStorage, error) {
	primaryUploadIdToSecondaryUploadIds := make(map[string][]string)
	return &ReplicationStorage{
		primaryStorage:                      primaryStorage,
		secondaryStorages:                   secondaryStorages,
		primaryUploadIdToSecondaryUploadIds: primaryUploadIdToSecondaryUploadIds,
		mapMutex:                            sync.Mutex{},
	}, nil
}

func (rs *ReplicationStorage) Start(ctx context.Context) error {
	err := rs.primaryStorage.Start(ctx)
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

func (rs *ReplicationStorage) Stop(ctx context.Context) error {
	err := rs.primaryStorage.Stop(ctx)
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

func (rs *ReplicationStorage) CreateBucket(ctx context.Context, bucket string) error {
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

func (rs *ReplicationStorage) DeleteBucket(ctx context.Context, bucket string) error {
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

func (rs *ReplicationStorage) ListBuckets(ctx context.Context) ([]Bucket, error) {
	return rs.primaryStorage.ListBuckets(ctx)
}

func (rs *ReplicationStorage) HeadBucket(ctx context.Context, bucket string) (*Bucket, error) {
	return rs.primaryStorage.HeadBucket(ctx, bucket)
}

func (rs *ReplicationStorage) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	return rs.primaryStorage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
}

func (rs *ReplicationStorage) HeadObject(ctx context.Context, bucket string, key string) (*Object, error) {
	return rs.primaryStorage.HeadObject(ctx, bucket, key)
}

func (rs *ReplicationStorage) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	return rs.primaryStorage.GetObject(ctx, bucket, key, startByte, endByte)
}

func (rs *ReplicationStorage) PutObject(ctx context.Context, bucket string, key string, reader io.Reader) error {
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

func (rs *ReplicationStorage) DeleteObject(ctx context.Context, bucket string, key string) error {
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

func (rs *ReplicationStorage) CreateMultipartUpload(ctx context.Context, bucket string, key string) (*InitiateMultipartUploadResult, error) {
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

func (rs *ReplicationStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, reader io.Reader) (*UploadPartResult, error) {
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

func (rs *ReplicationStorage) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
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

func (rs *ReplicationStorage) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
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
