package s3client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type s3ClientStorage struct {
	*lifecycle.ValidatedLifecycle
	s3Client *s3.Client
	tracer   trace.Tracer
}

// Compile-time check to ensure s3ClientStorage implements storage.Storage
var _ storage.Storage = (*s3ClientStorage)(nil)

func NewStorage(s3Client *s3.Client) (storage.Storage, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("S3ClientStorage")
	if err != nil {
		return nil, err
	}

	return &s3ClientStorage{
		ValidatedLifecycle: lifecycle,
		s3Client:           s3Client,
		tracer:             otel.Tracer("internal/storage/s3client"),
	}, nil
}

func (rs *s3ClientStorage) Start(ctx context.Context) error {
	return rs.ValidatedLifecycle.Start(ctx)
}

func (rs *s3ClientStorage) Stop(ctx context.Context) error {
	return rs.ValidatedLifecycle.Stop(ctx)
}

func (rs *s3ClientStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.CreateBucket")
	defer span.End()

	_, err := rs.s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName.String()),
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

func (rs *s3ClientStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.DeleteBucket")
	defer span.End()

	_, err := rs.s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName.String()),
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
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.ListBuckets")
	defer span.End()

	listBucketsResult, err := rs.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	buckets := sliceutils.Map(func(bucket types.Bucket) storage.Bucket {
		return storage.Bucket{
			Name:         storage.MustNewBucketName(*bucket.Name),
			CreationDate: *bucket.CreationDate,
		}
	}, listBucketsResult.Buckets)
	return buckets, nil
}

func (rs *s3ClientStorage) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.HeadBucket")
	defer span.End()

	_, err := rs.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName.String()),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.Bucket{
		Name:         bucketName,
		CreationDate: time.Time{},
	}, nil
}

func (rs *s3ClientStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.ListObjects")
	defer span.End()

	listObjectsResult, err := rs.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucketName.String()),
		Prefix:     opts.Prefix,
		Delimiter:  opts.Delimiter,
		StartAfter: opts.StartAfter,
		MaxKeys:    aws.Int32(opts.MaxKeys),
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
			Key:          storage.MustNewObjectKey(*object.Key),
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

func (rs *s3ClientStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (*storage.Object, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.HeadObject")
	defer span.End()

	headObjectResult, err := rs.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName.String()),
		Key:    aws.String(key.String()),
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

func (rs *s3ClientStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.GetObject")
	defer span.End()

	// If no ranges specified, get the entire object
	if len(ranges) == 0 {
		ranges = []storage.ByteRange{{Start: nil, End: nil}}
	}

	// First, get object metadata
	object, err := rs.HeadObject(ctx, bucketName, key)
	if err != nil {
		return nil, nil, err
	}

	// Get each range
	readers := []io.ReadCloser{}
	for _, byteRange := range ranges {
		startByte := byteRange.Start
		endByte := byteRange.End

		var awsRange *string = nil
		if startByte != nil && endByte != nil {
			r := fmt.Sprintf("bytes=%d-%d", *startByte, *endByte-1)
			awsRange = &r
		} else if startByte != nil {
			r := fmt.Sprintf("bytes=%d-", *startByte)
			awsRange = &r
		} else if endByte != nil {
			r := fmt.Sprintf("bytes=-%d", *endByte-1)
			awsRange = &r
		}
		getObjectResult, err := rs.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName.String()),
			Key:    aws.String(key.String()),
			Range:  awsRange,
		})
		var notFoundError *types.NotFound
		if err != nil && errors.As(err, &notFoundError) {
			// Close any readers we've already opened
			for _, r := range readers {
				r.Close()
			}
			return nil, nil, storage.ErrNoSuchBucket
		}
		if err != nil {
			// Close any readers we've already opened
			for _, r := range readers {
				r.Close()
			}
			return nil, nil, err
		}
		readers = append(readers, getObjectResult.Body)
	}

	return object, readers, nil
}

func (rs *s3ClientStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.PutObject")
	defer span.End()

	putObjectResult, err := rs.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketName.String()),
		Key:         aws.String(key.String()),
		ContentType: contentType,
		Body:        reader,
		// @TODO: Use checksumInput
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}

	return &storage.PutObjectResult{
		ETag:              putObjectResult.ETag,
		ChecksumCRC32:     putObjectResult.ChecksumCRC32,
		ChecksumCRC32C:    putObjectResult.ChecksumCRC32C,
		ChecksumCRC64NVME: putObjectResult.ChecksumCRC64NVME,
		ChecksumSHA1:      putObjectResult.ChecksumSHA1,
		ChecksumSHA256:    putObjectResult.ChecksumSHA256,
	}, nil
}

func (rs *s3ClientStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.DeleteObject")
	defer span.End()

	_, err := rs.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName.String()),
		Key:    aws.String(key.String()),
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

func (rs *s3ClientStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.CreateMultipartUpload")
	defer span.End()

	checksumTypeStr := types.ChecksumTypeFullObject
	if checksumType != nil {
		checksumTypeStr = types.ChecksumType(*checksumType)
	}
	initiateMultipartUploadResult, err := rs.s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(bucketName.String()),
		Key:          aws.String(key.String()),
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
		UploadId: storage.MustNewUploadId(*initiateMultipartUploadResult.UploadId),
	}, nil
}

func (rs *s3ClientStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.UploadPart")
	defer span.End()

	uploadPartResult, err := rs.s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName.String()),
		Key:        aws.String(key.String()),
		UploadId:   aws.String(uploadId.String()),
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
		ETag:              *uploadPartResult.ETag,
		ChecksumCRC32:     uploadPartResult.ChecksumCRC32,
		ChecksumCRC32C:    uploadPartResult.ChecksumCRC32C,
		ChecksumCRC64NVME: uploadPartResult.ChecksumCRC64NVME,
		ChecksumSHA1:      uploadPartResult.ChecksumSHA1,
		ChecksumSHA256:    uploadPartResult.ChecksumSHA256,
	}, nil
}

func (rs *s3ClientStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.CompleteMultipartUpload")
	defer span.End()

	completeMultipartUploadResult, err := rs.s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName.String()),
		Key:      aws.String(key.String()),
		UploadId: aws.String(uploadId.String()),
		// @TODO: Use checksumInput
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

func (rs *s3ClientStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.AbortMultipartUpload")
	defer span.End()

	_, err := rs.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName.String()),
		Key:      aws.String(key.String()),
		UploadId: aws.String(uploadId.String()),
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

func (rs *s3ClientStorage) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.ListMultipartUploads")
	defer span.End()

	listMultipartUploadsResult, err := rs.s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket:         aws.String(bucketName.String()),
		Prefix:         opts.Prefix,
		Delimiter:      opts.Delimiter,
		KeyMarker:      opts.KeyMarker,
		UploadIdMarker: opts.UploadIdMarker,
		MaxUploads:     aws.Int32(opts.MaxUploads),
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
			Key:       storage.MustNewObjectKey(*upload.Key),
			UploadId:  storage.MustNewUploadId(*upload.UploadId),
			Initiated: *upload.Initiated,
		}
	}, listMultipartUploadsResult.Uploads)
	commonPrefixes := sliceutils.Map(func(commonPrefix types.CommonPrefix) string {
		return *commonPrefix.Prefix
	}, listMultipartUploadsResult.CommonPrefixes)
	return &storage.ListMultipartUploadsResult{
		BucketName:         storage.MustNewBucketName(*listMultipartUploadsResult.Bucket),
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

func (rs *s3ClientStorage) ListParts(ctx context.Context, bucketName storage.BucketName, objectName storage.ObjectKey, uploadID storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.ListParts")
	defer span.End()

	listPartsResult, err := rs.s3Client.ListParts(ctx, &s3.ListPartsInput{
		Bucket:           aws.String(bucketName.String()),
		Key:              aws.String(objectName.String()),
		UploadId:         aws.String(uploadID.String()),
		PartNumberMarker: opts.PartNumberMarker,
		MaxParts:         aws.Int32(opts.MaxParts),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.ListPartsResult{
		BucketName:           storage.MustNewBucketName(*listPartsResult.Bucket),
		Key:                  storage.MustNewObjectKey(*listPartsResult.Key),
		UploadId:             storage.MustNewUploadId(*listPartsResult.UploadId),
		PartNumberMarker:     *listPartsResult.PartNumberMarker,
		NextPartNumberMarker: listPartsResult.NextPartNumberMarker,
		MaxParts:             *listPartsResult.MaxParts,
		IsTruncated:          *listPartsResult.IsTruncated,
		Parts: sliceutils.Map(func(part types.Part) *storage.MultipartPart {
			return &storage.MultipartPart{
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
