package s3client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/startstopvalidator"
)

type s3ClientStorage struct {
	s3Client           *s3.Client
	startStopValidator *startstopvalidator.StartStopValidator
}

func NewStorage(s3Client *s3.Client) (storage.Storage, error) {
	startStopValidator, err := startstopvalidator.New("S3ClientStorage")
	if err != nil {
		return nil, err
	}

	return &s3ClientStorage{
		s3Client:           s3Client,
		startStopValidator: startStopValidator,
	}, nil
}

func (rs *s3ClientStorage) Start(ctx context.Context) error {
	err := rs.startStopValidator.Start()
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) Stop(ctx context.Context) error {
	err := rs.startStopValidator.Stop()
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) CreateBucket(ctx context.Context, bucket string) error {
	_, err := rs.s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	var bucketAlreadyExistsError *types.BucketAlreadyExists
	if err != nil && errors.As(err, &bucketAlreadyExistsError) {
		return storage.ErrBucketAlreadyExists
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) DeleteBucket(ctx context.Context, bucket string) error {
	_, err := rs.s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return storage.ErrNoSuchBucket
	}
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "BucketNotEmpty" {
		return storage.ErrBucketNotEmpty
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	listBucketsResult, err := rs.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	buckets := sliceutils.Map(func(bucket types.Bucket) storage.Bucket {
		return storage.Bucket{
			Name:         *bucket.Name,
			CreationDate: *bucket.CreationDate,
		}
	}, listBucketsResult.Buckets)
	return buckets, nil
}

func (rs *s3ClientStorage) HeadBucket(ctx context.Context, bucket string) (*storage.Bucket, error) {
	_, err := rs.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.Bucket{
		Name:         bucket,
		CreationDate: time.Time{},
	}, nil
}

func (rs *s3ClientStorage) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int32) (*storage.ListBucketResult, error) {
	listObjectsResult, err := rs.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		Prefix:     aws.String(prefix),
		Delimiter:  aws.String(delimiter),
		StartAfter: aws.String(startAfter),
		MaxKeys:    aws.Int32(maxKeys),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	objects := sliceutils.Map(func(object types.Object) storage.Object {
		return storage.Object{
			Key:          *object.Key,
			LastModified: *object.LastModified,
			ETag:         *object.ETag,
			// @TODO: checksums
			Size: *object.Size,
		}
	}, listObjectsResult.Contents)
	commonPrefixes := sliceutils.Map(func(commonPrefix types.CommonPrefix) string {
		return *commonPrefix.Prefix
	}, listObjectsResult.CommonPrefixes)
	return &storage.ListBucketResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    *listObjectsResult.IsTruncated,
	}, nil
}

func (rs *s3ClientStorage) HeadObject(ctx context.Context, bucket string, key string) (*storage.Object, error) {
	headObjectResult, err := rs.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.Object{
		Key:               key,
		LastModified:      *headObjectResult.LastModified,
		ETag:              *headObjectResult.ETag,
		ChecksumCRC32:     headObjectResult.ChecksumCRC32,
		ChecksumCRC32C:    headObjectResult.ChecksumCRC32C,
		ChecksumCRC64NVME: headObjectResult.ChecksumCRC64NVME,
		ChecksumSHA1:      headObjectResult.ChecksumSHA1,
		ChecksumSHA256:    headObjectResult.ChecksumSHA256,
		ChecksumType:      (*string)(&headObjectResult.ChecksumType),
		Size:              *headObjectResult.ContentLength,
	}, nil
}

func (rs *s3ClientStorage) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	var byteRange *string = nil
	if startByte != nil && endByte != nil {
		r := fmt.Sprintf("bytes=%d-%d", *startByte, *endByte-1)
		byteRange = &r
	} else if startByte != nil {
		r := fmt.Sprintf("bytes=%d-", *startByte)
		byteRange = &r
	} else if endByte != nil {
		r := fmt.Sprintf("bytes=-%d", *endByte-1)
		byteRange = &r
	}
	getObjectResult, err := rs.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  byteRange,
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}

	return getObjectResult.Body, nil
}

func (rs *s3ClientStorage) PutObject(ctx context.Context, bucket string, key string, contentType *string, reader io.Reader, checksumInput storage.ChecksumInput) error {
	_, err := rs.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: contentType,
		Body:        reader,
		// @TODO: Use checksumInput
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return storage.ErrNoSuchBucket
	}

	return err
}

func (rs *s3ClientStorage) DeleteObject(ctx context.Context, bucket string, key string) error {
	_, err := rs.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return storage.ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) CreateMultipartUpload(ctx context.Context, bucket string, key string, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	checksumTypeStr := types.ChecksumTypeFullObject
	if checksumType != nil {
		checksumTypeStr = types.ChecksumType(*checksumType)
	}
	initiateMultipartUploadResult, err := rs.s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ContentType:  contentType,
		ChecksumType: checksumTypeStr,
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.InitiateMultipartUploadResult{
		UploadId: *initiateMultipartUploadResult.UploadId,
	}, nil
}

func (rs *s3ClientStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, data io.Reader, checksumInput storage.ChecksumInput) (*storage.UploadPartResult, error) {
	uploadPartResult, err := rs.s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadId),
		PartNumber: aws.Int32(partNumber),
		Body:       data,
		// @TODO: Use checksumInput
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.UploadPartResult{
		ETag: *uploadPartResult.ETag,
	}, nil
}

func (rs *s3ClientStorage) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*storage.CompleteMultipartUploadResult, error) {
	completeMultipartUploadResult, err := rs.s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.CompleteMultipartUploadResult{
		Location:          *completeMultipartUploadResult.Location,
		ETag:              *completeMultipartUploadResult.ETag,
		ChecksumCRC32:     completeMultipartUploadResult.ChecksumCRC32,
		ChecksumCRC32C:    completeMultipartUploadResult.ChecksumCRC32C,
		ChecksumCRC64NVME: completeMultipartUploadResult.ChecksumCRC64NVME,
		ChecksumSHA1:      completeMultipartUploadResult.ChecksumSHA1,
		ChecksumSHA256:    completeMultipartUploadResult.ChecksumSHA256,
		ChecksumType:      (*string)(&completeMultipartUploadResult.ChecksumType),
	}, nil
}

func (rs *s3ClientStorage) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	_, err := rs.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return storage.ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) ListMultipartUploads(ctx context.Context, bucket string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*storage.ListMultipartUploadsResult, error) {
	listMultipartUploadsResult, err := rs.s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket:         aws.String(bucket),
		Prefix:         aws.String(prefix),
		Delimiter:      aws.String(delimiter),
		KeyMarker:      aws.String(keyMarker),
		UploadIdMarker: aws.String(uploadIdMarker),
		MaxUploads:     aws.Int32(maxUploads),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}

	uploads := sliceutils.Map(func(upload types.MultipartUpload) storage.Upload {
		return storage.Upload{
			Key:       *upload.Key,
			UploadId:  *upload.UploadId,
			Initiated: *upload.Initiated,
		}
	}, listMultipartUploadsResult.Uploads)
	commonPrefixes := sliceutils.Map(func(commonPrefix types.CommonPrefix) string {
		return *commonPrefix.Prefix
	}, listMultipartUploadsResult.CommonPrefixes)
	return &storage.ListMultipartUploadsResult{
		Bucket:             *listMultipartUploadsResult.Bucket,
		KeyMarker:          *listMultipartUploadsResult.KeyMarker,
		UploadIdMarker:     *listMultipartUploadsResult.UploadIdMarker,
		Prefix:             *listMultipartUploadsResult.Prefix,
		Delimiter:          *listMultipartUploadsResult.Delimiter,
		NextKeyMarker:      *listMultipartUploadsResult.NextKeyMarker,
		NextUploadIdMarker: *listMultipartUploadsResult.NextUploadIdMarker,
		MaxUploads:         *listMultipartUploadsResult.MaxUploads,
		CommonPrefixes:     commonPrefixes,
		Uploads:            uploads,
		IsTruncated:        *listMultipartUploadsResult.IsTruncated,
	}, nil
}

func (rs *s3ClientStorage) ListParts(ctx context.Context, bucket string, key string, uploadId string, partNumberMarker string, maxParts int32) (*storage.ListPartsResult, error) {
	listPartsResult, err := rs.s3Client.ListParts(ctx, &s3.ListPartsInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(key),
		UploadId:         aws.String(uploadId),
		PartNumberMarker: aws.String(partNumberMarker),
		MaxParts:         aws.Int32(maxParts),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.ListPartsResult{
		Bucket:               *listPartsResult.Bucket,
		Key:                  *listPartsResult.Key,
		UploadId:             *listPartsResult.UploadId,
		PartNumberMarker:     *listPartsResult.PartNumberMarker,
		NextPartNumberMarker: listPartsResult.NextPartNumberMarker,
		MaxParts:             *listPartsResult.MaxParts,
		IsTruncated:          *listPartsResult.IsTruncated,
		Parts: sliceutils.Map(func(part types.Part) *storage.Part {
			return &storage.Part{
				ETag:              *part.ETag,
				ChecksumCRC32:     part.ChecksumCRC32,
				ChecksumCRC32C:    part.ChecksumCRC32C,
				ChecksumCRC64NVME: part.ChecksumCRC64NVME,
				ChecksumSHA1:      part.ChecksumSHA1,
				ChecksumSHA256:    part.ChecksumSHA256,
				LastModified:      *part.LastModified,
				PartNumber:        *part.PartNumber,
				Size:              *part.Size,
			}
		}, listPartsResult.Parts),
	}, nil
}
