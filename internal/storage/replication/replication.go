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

func (rs *replicationStorage) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int32) (*storage.ListBucketResult, error) {
	return rs.primaryStorage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
}

func (rs *replicationStorage) HeadObject(ctx context.Context, bucket string, key string) (*storage.Object, error) {
	return rs.primaryStorage.HeadObject(ctx, bucket, key)
}

func (rs *replicationStorage) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	return rs.primaryStorage.GetObject(ctx, bucket, key, startByte, endByte)
}

func (rs *replicationStorage) PutObject(ctx context.Context, bucket string, key string, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	readers, writer, closer := ioutils.PipeWriterIntoMultipleReaders(len(rs.secondaryStorages) + 1)

	doneChan := make(chan *storage.PutObjectResult, 1)
	errChan := make(chan error, 1)
	go func() {
		result, err := rs.primaryStorage.PutObject(ctx, bucket, key, contentType, readers[len(rs.secondaryStorages)], checksumInput)
		if err != nil {
			errChan <- err
			return
		}
		doneChan <- result
	}()

	var doneChannels []chan *storage.PutObjectResult = nil
	var errChannels []chan error = nil
	for range rs.secondaryStorages {
		doneChannels = append(doneChannels, make(chan *storage.PutObjectResult))
		errChannels = append(errChannels, make(chan error))
	}

	for i, secondaryStorage := range rs.secondaryStorages {
		go func() {
			result, err := secondaryStorage.PutObject(ctx, bucket, key, contentType, readers[i], checksumInput)
			if err != nil {
				errChannels[i] <- err
				return
			}
			doneChannels[i] <- result
		}()
	}

	_, err := io.Copy(writer, reader)
	if err != nil {
		closer.Close()
		return nil, err
	}
	closer.Close()

	var putObjectResult *storage.PutObjectResult
	select {
	case putObjectResult = <-doneChan:
	case err := <-errChan:
		if err != nil {
			return nil, err
		}
	}

	for i := range rs.secondaryStorages {
		select {
		case <-doneChannels[i]:
		case err := <-errChannels[i]:
			if err != nil {
				return nil, err
			}
		}
	}
	return putObjectResult, nil
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

func (rs *replicationStorage) CreateMultipartUpload(ctx context.Context, bucket string, key string, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	initiateMultipartUploadResult, err := rs.primaryStorage.CreateMultipartUpload(ctx, bucket, key, contentType, checksumType)
	if err != nil {
		return nil, err
	}
	var secondaryUploadIds []string = []string{}
	for _, secondaryStorage := range rs.secondaryStorages {
		initiateMultipartUploadResult, err := secondaryStorage.CreateMultipartUpload(ctx, bucket, key, contentType, checksumType)
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

func (rs *replicationStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	readers, writer, closer := ioutils.PipeWriterIntoMultipleReaders(len(rs.secondaryStorages) + 1)

	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	rs.mapMutex.Unlock()

	uploadPartResultChan := make(chan *storage.UploadPartResult, 1)
	errChan := make(chan error, 1)
	go func() {
		uploadPartResult, err := rs.primaryStorage.UploadPart(ctx, bucket, key, uploadId, partNumber, readers[len(rs.secondaryStorages)], checksumInput)
		if err != nil {
			errChan <- err
			return
		}
		uploadPartResultChan <- uploadPartResult
	}()

	var doneChannels []chan struct{} = nil
	var errChannels []chan error = nil
	for range rs.secondaryStorages {
		doneChannels = append(doneChannels, make(chan struct{}))
		errChannels = append(errChannels, make(chan error))
	}

	for i, secondaryStorage := range rs.secondaryStorages {
		go func() {
			_, err := secondaryStorage.UploadPart(ctx, bucket, key, secondaryUploadIds[i], partNumber, readers[i], checksumInput)
			if err != nil {
				errChannels[i] <- err
				return
			}
			doneChannels[i] <- struct{}{}
		}()
	}

	_, err := io.Copy(writer, reader)
	if err != nil {
		return nil, err
	}
	err = closer.Close()
	if err != nil {
		return nil, err
	}

	var uploadPartResult *storage.UploadPartResult
	select {
	case uploadPartResult = <-uploadPartResultChan:
	case err := <-errChan:
		if err != nil {
			return nil, err
		}
	}

	for i := range rs.secondaryStorages {
		select {
		case <-doneChannels[i]:
		case err := <-errChannels[i]:
			if err != nil {
				return nil, err
			}
		}
	}
	return uploadPartResult, nil
}

func (rs *replicationStorage) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	completeMultipartUploadResult, err := rs.primaryStorage.CompleteMultipartUpload(ctx, bucket, key, uploadId, checksumInput)
	if err != nil {
		return nil, err
	}
	rs.mapMutex.Lock()
	secondaryUploadIds := rs.primaryUploadIdToSecondaryUploadIds[uploadId]
	for i, secondaryStorage := range rs.secondaryStorages {
		_, err := secondaryStorage.CompleteMultipartUpload(ctx, bucket, key, secondaryUploadIds[i], checksumInput)
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

func (rs *replicationStorage) ListMultipartUploads(ctx context.Context, bucket string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*storage.ListMultipartUploadsResult, error) {
	return rs.primaryStorage.ListMultipartUploads(ctx, bucket, prefix, delimiter, keyMarker, uploadIdMarker, maxUploads)
}

func (rs *replicationStorage) ListParts(ctx context.Context, bucket string, key string, uploadId string, partNumberMarker string, maxParts int32) (*storage.ListPartsResult, error) {
	return rs.primaryStorage.ListParts(ctx, bucket, key, uploadId, partNumberMarker, maxParts)
}
