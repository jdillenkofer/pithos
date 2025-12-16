package migrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
)

var ErrDestinationNotEmpty = errors.New("destination storage not empty")

func MigrateStorage(ctx context.Context, source storage.Storage, destination storage.Storage) error {
	missingBuckets, err := determineMissingBuckets(ctx, source, destination)
	if err != nil {
		return err
	}
	err = createMissingBuckets(ctx, missingBuckets, destination)
	if err != nil {
		return err
	}
	allSourceBuckets, err := source.ListBuckets(ctx)
	if err != nil {
		return err
	}
	for i, sourceBucket := range allSourceBuckets {
		slog.Info(fmt.Sprintf("Migrating bucket \"%s\" (%d/%d items [%.2f%%])", sourceBucket.Name, i, len(allSourceBuckets), float64(i)/float64(len(allSourceBuckets))*100.0))
		err = migrateObjectsOfBucketFromSourceStorageToDestinationStorage(ctx, source, destination, sourceBucket.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

func determineMissingBuckets(ctx context.Context, source, destination storage.Storage) ([]storage.Bucket, error) {
	allSourceBuckets, err := source.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	allDestinationBuckets, err := destination.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	existingDestinationBucketsByBucketName := map[string]storage.Bucket{}
	for _, destinationBucket := range allDestinationBuckets {
		existingDestinationBucketsByBucketName[destinationBucket.Name.String()] = destinationBucket
	}

	missingSourceBuckets := []storage.Bucket{}
	for _, sourceBucket := range allSourceBuckets {
		_, bucketAlreadyExists := existingDestinationBucketsByBucketName[sourceBucket.Name.String()]
		if !bucketAlreadyExists {
			missingSourceBuckets = append(missingSourceBuckets, sourceBucket)
		}
	}
	return missingSourceBuckets, nil
}

func createMissingBuckets(ctx context.Context, missingBuckets []storage.Bucket, destination storage.Storage) error {
	for _, missingBucket := range missingBuckets {
		err := destination.CreateBucket(ctx, missingBucket.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

func migrateObjectsOfBucketFromSourceStorageToDestinationStorage(ctx context.Context, source, destination storage.Storage, bucketName storage.BucketName) error {
	destinationObjects, err := storage.ListAllObjectsOfBucket(ctx, destination, bucketName)
	if err != nil {
		return err
	}
	if len(destinationObjects) != 0 {
		return ErrDestinationNotEmpty
	}
	sourceObjects, err := storage.ListAllObjectsOfBucket(ctx, source, bucketName)
	if err != nil {
		return err
	}

	var copiedBytes int64 = 0
	var totalBytes int64 = 0
	for _, sourceObject := range sourceObjects {
		totalBytes += sourceObject.Size
	}
	for i, sourceObject := range sourceObjects {
		slog.Info(fmt.Sprintf("Migrating object \"%s\" from bucket \"%s\" (%d/%d items [%.2f%%]; %d/%d bytes [%.2f%%])", sourceObject.Key, bucketName, i, len(sourceObjects), (float64(i)+1.0)/float64(len(sourceObjects))*100.0, copiedBytes, totalBytes, float64(copiedBytes)*100.0/float64(totalBytes)))
		err1 := migrateSingleObject(ctx, source, destination, bucketName, sourceObject)
		if err1 != nil {
			return err1
		}
		copiedBytes += sourceObject.Size
	}
	return nil
}

func migrateSingleObject(ctx context.Context, source, destination storage.Storage, bucketName storage.BucketName, sourceObject storage.Object) error {
	_, readers, err := source.GetObject(ctx, bucketName, sourceObject.Key, nil)
	if err != nil {
		return err
	}
	if len(readers) == 0 {
		return fmt.Errorf("no readers returned")
	}
	obj := readers[0]
	defer obj.Close()

	tempFile, err := os.CreateTemp("", "pithos-migrator-*")
	if err != nil {
		return err
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	_, err = ioutils.Copy(tempFile, obj)
	if err != nil {
		return err
	}
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	adapter := NewStorageToS3UploadAPIClientAdapter(destination)
	uploader := manager.NewUploader(adapter, func(u *manager.Uploader) {
		u.Concurrency = 1
	})
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: ptrutils.ToPtr(bucketName.String()),
		Key:    ptrutils.ToPtr(sourceObject.Key.String()),
		Body:   tempFile,
	})
	if err != nil {
		return err
	}
	return nil
}

// Adapter from storage to manager.UploadAPIClient
type StorageToS3UploadAPIClientAdapter struct {
	storage storage.Storage
}

func NewStorageToS3UploadAPIClientAdapter(storage storage.Storage) *StorageToS3UploadAPIClientAdapter {
	return &StorageToS3UploadAPIClientAdapter{
		storage: storage,
	}
}

func (a *StorageToS3UploadAPIClientAdapter) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	result, err := a.storage.CreateMultipartUpload(ctx, storage.MustNewBucketName(*input.Bucket), storage.MustNewObjectKey(*input.Key), input.ContentType, nil)
	if err != nil {
		return nil, err
	}

	return &s3.CreateMultipartUploadOutput{
		Bucket:   input.Bucket,
		Key:      input.Key,
		UploadId: aws.String(result.UploadId.String()),
	}, nil
}

func (a *StorageToS3UploadAPIClientAdapter) UploadPart(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	result, err := a.storage.UploadPart(ctx, storage.MustNewBucketName(*input.Bucket), storage.MustNewObjectKey(*input.Key), storage.MustNewUploadId(*input.UploadId), *input.PartNumber, input.Body, nil)
	if err != nil {
		return nil, err
	}

	return &s3.UploadPartOutput{
		ETag:              &result.ETag,
		ChecksumCRC32:     result.ChecksumCRC32,
		ChecksumCRC32C:    result.ChecksumCRC32C,
		ChecksumCRC64NVME: result.ChecksumCRC64NVME,
		ChecksumSHA1:      result.ChecksumSHA1,
		ChecksumSHA256:    result.ChecksumSHA256,
	}, nil
}

func (a *StorageToS3UploadAPIClientAdapter) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	result, err := a.storage.CompleteMultipartUpload(ctx, storage.MustNewBucketName(*input.Bucket), storage.MustNewObjectKey(*input.Key), storage.MustNewUploadId(*input.UploadId), nil)
	if err != nil {
		return nil, err
	}

	return &s3.CompleteMultipartUploadOutput{
		Bucket:         input.Bucket,
		Key:            input.Key,
		Location:       &result.Location,
		ETag:           &result.ETag,
		ChecksumCRC32:  result.ChecksumCRC32,
		ChecksumCRC32C: result.ChecksumCRC32C,
		ChecksumSHA1:   result.ChecksumSHA1,
		ChecksumSHA256: result.ChecksumSHA256,
	}, nil
}

func (a *StorageToS3UploadAPIClientAdapter) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	err := a.storage.AbortMultipartUpload(ctx, storage.MustNewBucketName(*input.Bucket), storage.MustNewObjectKey(*input.Key), storage.MustNewUploadId(*input.UploadId))
	if err != nil {
		return nil, err
	}

	return &s3.AbortMultipartUploadOutput{}, nil
}

func (a *StorageToS3UploadAPIClientAdapter) PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	result, err := a.storage.PutObject(ctx, storage.MustNewBucketName(*input.Bucket), storage.MustNewObjectKey(*input.Key), input.ContentType, input.Body, nil)
	if err != nil {
		return nil, err
	}

	return &s3.PutObjectOutput{
		ETag:           result.ETag,
		ChecksumCRC32:  result.ChecksumCRC32,
		ChecksumCRC32C: result.ChecksumCRC32C,
		ChecksumSHA1:   result.ChecksumSHA1,
		ChecksumSHA256: result.ChecksumSHA256,
	}, nil
}
