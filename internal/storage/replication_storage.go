package storage

import (
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

func (rs *ReplicationStorage) Start() error {
	err := rs.primaryStorage.Start()
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *ReplicationStorage) Stop() error {
	err := rs.primaryStorage.Stop()
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.Stop()
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *ReplicationStorage) CreateBucket(bucket string) error {
	err := rs.primaryStorage.CreateBucket(bucket)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.CreateBucket(bucket)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *ReplicationStorage) DeleteBucket(bucket string) error {
	err := rs.primaryStorage.DeleteBucket(bucket)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.DeleteBucket(bucket)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *ReplicationStorage) ListBuckets() ([]Bucket, error) {
	return rs.primaryStorage.ListBuckets()
}

func (rs *ReplicationStorage) HeadBucket(bucket string) (*Bucket, error) {
	return rs.primaryStorage.HeadBucket(bucket)
}

func (rs *ReplicationStorage) ListObjects(bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	return rs.primaryStorage.ListObjects(bucket, prefix, delimiter, startAfter, maxKeys)
}

func (rs *ReplicationStorage) HeadObject(bucket string, key string) (*Object, error) {
	return rs.primaryStorage.HeadObject(bucket, key)
}

func (rs *ReplicationStorage) GetObject(bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	return rs.primaryStorage.GetObject(bucket, key, startByte, endByte)
}

func (rs *ReplicationStorage) PutObject(bucket string, key string, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(data)

	err = rs.primaryStorage.PutObject(bucket, key, byteReadSeekCloser)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		_, err = byteReadSeekCloser.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		err = secondaryStorage.PutObject(bucket, key, byteReadSeekCloser)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *ReplicationStorage) DeleteObject(bucket string, key string) error {
	err := rs.primaryStorage.DeleteObject(bucket, key)
	if err != nil {
		return err
	}
	for _, secondaryStorage := range rs.secondaryStorages {
		err = secondaryStorage.DeleteObject(bucket, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *ReplicationStorage) CreateMultipartUpload(bucket string, key string) (*InitiateMultipartUploadResult, error) {
	initiateMultipartUploadResult, err := rs.primaryStorage.CreateMultipartUpload(bucket, key)
	if err != nil {
		return nil, err
	}
	var secondaryUploadIds []string = []string{}
	for _, secondaryStorage := range rs.secondaryStorages {
		initiateMultipartUploadResult, err := secondaryStorage.CreateMultipartUpload(bucket, key)
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

func (rs *ReplicationStorage) UploadPart(bucket string, key string, uploadId string, partNumber int32, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(data)

	err = rs.primaryStorage.UploadPart(bucket, key, uploadId, partNumber, byteReadSeekCloser)
	if err != nil {
		return err
	}

	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	rs.mapMutex.Unlock()

	for i, secondaryStorage := range rs.secondaryStorages {
		_, err = byteReadSeekCloser.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		err := secondaryStorage.UploadPart(bucket, key, secondaryUploadIds[i], partNumber, byteReadSeekCloser)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *ReplicationStorage) CompleteMultipartUpload(bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	completeMultipartUploadResult, err := rs.primaryStorage.CompleteMultipartUpload(bucket, key, uploadId)
	if err != nil {
		return nil, err
	}
	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	for i, secondaryStorage := range rs.secondaryStorages {
		_, err := secondaryStorage.CompleteMultipartUpload(bucket, key, secondaryUploadIds[i])
		if err != nil {
			return nil, err
		}
	}
	delete(rs.primaryUploadIdToSecondaryUploadIds, uploadId)
	rs.mapMutex.Unlock()
	return completeMultipartUploadResult, nil
}

func (rs *ReplicationStorage) AbortMultipartUpload(bucket string, key string, uploadId string) error {
	err := rs.primaryStorage.AbortMultipartUpload(bucket, key, uploadId)
	if err != nil {
		return err
	}
	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	for i, secondaryStorage := range rs.secondaryStorages {
		err := secondaryStorage.AbortMultipartUpload(bucket, key, secondaryUploadIds[i])
		if err != nil {
			return err
		}
	}
	delete(rs.primaryUploadIdToSecondaryUploadIds, uploadId)
	rs.mapMutex.Unlock()
	return nil
}
