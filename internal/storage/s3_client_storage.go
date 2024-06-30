package storage

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
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
)

type S3ClientStorage struct {
	s3Client *s3.Client
}

func NewS3ClientStorage(s3Client *s3.Client) (*S3ClientStorage, error) {
	return &S3ClientStorage{
		s3Client: s3Client,
	}, nil
}

func (rs *S3ClientStorage) Start() error {
	return nil
}

func (rs *S3ClientStorage) Stop() error {
	return nil
}

func (rs *S3ClientStorage) CreateBucket(bucket string) error {
	_, err := rs.s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	var bucketAlreadyExistsError *types.BucketAlreadyExists
	if err != nil && errors.As(err, &bucketAlreadyExistsError) {
		return ErrBucketAlreadyExists
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *S3ClientStorage) DeleteBucket(bucket string) error {
	_, err := rs.s3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return ErrNoSuchBucket
	}
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "BucketNotEmpty" {
		return ErrBucketNotEmpty
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *S3ClientStorage) ListBuckets() ([]Bucket, error) {
	listBucketsResult, err := rs.s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	buckets := sliceutils.Map(func(bucket types.Bucket) Bucket {
		return Bucket{
			Name:         *bucket.Name,
			CreationDate: *bucket.CreationDate,
		}
	}, listBucketsResult.Buckets)
	return buckets, nil
}

func (rs *S3ClientStorage) HeadBucket(bucket string) (*Bucket, error) {
	_, err := rs.s3Client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &Bucket{
		Name:         bucket,
		CreationDate: time.Time{},
	}, nil
}

func (rs *S3ClientStorage) ListObjects(bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	listObjectsResult, err := rs.s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		Prefix:     aws.String(prefix),
		Delimiter:  aws.String(delimiter),
		StartAfter: aws.String(startAfter),
		MaxKeys:    aws.Int32(int32(maxKeys)),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	objects := sliceutils.Map(func(object types.Object) Object {
		return Object{
			Key:          *object.Key,
			LastModified: *object.LastModified,
			ETag:         *object.ETag,
			Size:         *object.Size,
		}
	}, listObjectsResult.Contents)
	commonPrefixes := sliceutils.Map(func(commonPrefix types.CommonPrefix) string {
		return *commonPrefix.Prefix
	}, listObjectsResult.CommonPrefixes)
	return &ListBucketResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    *listObjectsResult.IsTruncated,
	}, nil
}

func (rs *S3ClientStorage) HeadObject(bucket string, key string) (*Object, error) {
	headObjectResult, err := rs.s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &Object{
		Key:          key,
		LastModified: *headObjectResult.LastModified,
		ETag:         *headObjectResult.ETag,
		Size:         *headObjectResult.ContentLength,
	}, nil
}

func (rs *S3ClientStorage) GetObject(bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
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
	getObjectResult, err := rs.s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  byteRange,
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(getObjectResult.Body)
	if err != nil {
		return nil, err
	}
	return ioutils.NewByteReadSeekCloser(data), nil
}

func (rs *S3ClientStorage) PutObject(bucket string, key string, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	_, err = rs.s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   ioutils.NewByteReadSeekCloser(data),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return ErrNoSuchBucket
	}

	return err
}

func (rs *S3ClientStorage) DeleteObject(bucket string, key string) error {
	_, err := rs.s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *S3ClientStorage) CreateMultipartUpload(bucket string, key string) (*InitiateMultipartUploadResult, error) {
	initiateMultipartUploadResult, err := rs.s3Client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &InitiateMultipartUploadResult{
		UploadId: *initiateMultipartUploadResult.UploadId,
	}, nil
}

func (rs *S3ClientStorage) UploadPart(bucket string, key string, uploadId string, partNumber int32, data io.Reader) error {
	_, err := rs.s3Client.UploadPart(context.TODO(), &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadId),
		PartNumber: aws.Int32(partNumber),
		Body:       data,
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *S3ClientStorage) CompleteMultipartUpload(bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	completeMultipartUploadResult, err := rs.s3Client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &CompleteMultipartUploadResult{
		Location:       *completeMultipartUploadResult.Location,
		ETag:           *completeMultipartUploadResult.ETag,
		ChecksumSHA1:   *completeMultipartUploadResult.ChecksumSHA1,
		ChecksumCRC32:  *completeMultipartUploadResult.ChecksumCRC32,
		ChecksumCRC32C: *completeMultipartUploadResult.ChecksumCRC32C,
		ChecksumSHA256: *completeMultipartUploadResult.ChecksumSHA256,
	}, nil
}

func (rs *S3ClientStorage) AbortMultipartUpload(bucket string, key string, uploadId string) error {
	_, err := rs.s3Client.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}
