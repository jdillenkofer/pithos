package storage

import (
	"context"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
)

type Bucket struct {
	Name         string
	CreationDate time.Time
}

type Object struct {
	Key               string
	ContentType       *string
	LastModified      time.Time
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumType      *string
	Size              int64
}

type ListBucketResult struct {
	Objects        []Object
	CommonPrefixes []string
	IsTruncated    bool
}

type InitiateMultipartUploadResult struct {
	UploadId string
}

type UploadPartResult struct {
	ETag string
}

type CompleteMultipartUploadResult struct {
	Location          string
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumType      *string
}

type Upload struct {
	Key       string
	UploadId  string
	Initiated time.Time
}

type ListMultipartUploadsResult struct {
	Bucket             string
	KeyMarker          string
	UploadIdMarker     string
	NextKeyMarker      string
	Prefix             string
	Delimiter          string
	NextUploadIdMarker string
	MaxUploads         int32
	CommonPrefixes     []string
	Uploads            []Upload
	IsTruncated        bool
}

type Part struct {
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	LastModified      time.Time
	PartNumber        int32
	Size              int64
}

type ListPartsResult struct {
	Bucket               string
	Key                  string
	UploadId             string
	PartNumberMarker     string
	NextPartNumberMarker *string
	MaxParts             int32
	IsTruncated          bool
	Parts                []*Part
}

type ChecksumInput struct {
	ChecksumType      *string
	ChecksumAlgorithm *string
	ETag              *string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
}

var ErrNoSuchBucket error = metadatastore.ErrNoSuchBucket
var ErrBucketAlreadyExists error = metadatastore.ErrBucketAlreadyExists
var ErrBucketNotEmpty error = metadatastore.ErrBucketNotEmpty
var ErrNoSuchKey error = metadatastore.ErrNoSuchKey
var ErrBadDigest error = metadatastore.ErrBadDigest
var ErrNotImplemented error = metadatastore.ErrNotImplemented

type Storage interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	CreateBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]Bucket, error)
	HeadBucket(ctx context.Context, bucket string) (*Bucket, error)
	ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int32) (*ListBucketResult, error)
	HeadObject(ctx context.Context, bucket string, key string) (*Object, error)
	GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket string, key string, contentType *string, data io.Reader, checksumInput ChecksumInput) error
	DeleteObject(ctx context.Context, bucket string, key string) error
	CreateMultipartUpload(ctx context.Context, bucket string, key string, contentType *string, checksumType *string) (*InitiateMultipartUploadResult, error)
	UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, data io.Reader, checksumInput ChecksumInput) (*UploadPartResult, error)
	CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error
	ListMultipartUploads(ctx context.Context, bucket string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*ListMultipartUploadsResult, error)
	ListParts(ctx context.Context, bucket string, key string, uploadId string, partNumberMarker string, maxParts int32) (*ListPartsResult, error)
}

func ListAllObjectsOfBucket(ctx context.Context, storage Storage, bucketName string) ([]Object, error) {
	allObjects := []Object{}
	startAfter := ""
	for {
		listBucketResult, err := storage.ListObjects(ctx, bucketName, "", "", startAfter, 1000)
		if err != nil {
			return nil, err
		}
		allObjects = append(allObjects, listBucketResult.Objects...)
		if !listBucketResult.IsTruncated {
			break
		}
		startAfter = listBucketResult.Objects[len(listBucketResult.Objects)-1].Key
	}
	return allObjects, nil
}

func Tester(storage Storage, buckets []string, content []byte) error {
	ctx := context.Background()
	err := storage.Start(ctx)
	if err != nil {
		return err
	}
	defer storage.Stop(ctx)

	for _, bucketName := range buckets {
		key := "test"
		data := ioutils.NewByteReadSeekCloser(content)

		err = storage.CreateBucket(ctx, bucketName)
		if err != nil {
			return err
		}

		bucket, err := storage.HeadBucket(ctx, bucketName)
		if err != nil {
			return err
		}

		if bucketName != bucket.Name {
			return errors.New("invalid bucketName")
		}

		buckets, err := storage.ListBuckets(ctx)
		if err != nil {
			return err
		}

		if len(buckets) != 1 {
			return errors.New("expected 1 bucket got " + strconv.Itoa(len(buckets)))
		}

		if bucketName != buckets[0].Name {
			return errors.New("invalid bucketName")
		}

		err = storage.PutObject(ctx, bucketName, key, nil, data, ChecksumInput{})
		if err != nil {
			return err
		}

		object, err := storage.HeadObject(ctx, bucketName, key)
		if err != nil {
			return err
		}

		if object.Size != int64(len(content)) {
			return errors.New("invalid blob length")
		}

		listBucketResult, err := storage.ListObjects(ctx, bucketName, "", "", "", 1000)
		if err != nil {
			return err
		}

		if len(listBucketResult.Objects) != 1 {
			return errors.New("invalid objects length")
		}

		if key != listBucketResult.Objects[0].Key {
			return errors.New("invalid object key")
		}

		err = storage.DeleteObject(ctx, bucketName, key)
		if err != nil {
			return err
		}

		initiateMultipartUploadResult, err := storage.CreateMultipartUpload(ctx, bucketName, key, nil, nil)
		if err != nil {
			return err
		}

		_, err = data.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}

		_, err = storage.UploadPart(ctx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, data, ChecksumInput{})
		if err != nil {
			return err
		}

		_, err = storage.CompleteMultipartUpload(ctx, bucketName, key, initiateMultipartUploadResult.UploadId)
		if err != nil {
			return err
		}

		err = storage.DeleteObject(ctx, bucketName, key)
		if err != nil {
			return err
		}

		initiateMultipartUploadResult, err = storage.CreateMultipartUpload(ctx, bucketName, key, nil, nil)
		if err != nil {
			return err
		}

		_, err = data.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}

		_, err = storage.UploadPart(ctx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, data, ChecksumInput{})
		if err != nil {
			return err
		}

		err = storage.AbortMultipartUpload(ctx, bucketName, key, initiateMultipartUploadResult.UploadId)
		if err != nil {
			return err
		}

		err = storage.DeleteBucket(ctx, bucketName)
		if err != nil {
			return err
		}

		buckets, err = storage.ListBuckets(ctx)
		if err != nil {
			return err
		}

		if len(buckets) != 0 {
			return errors.New("expected 0 bucket got " + strconv.Itoa(len(buckets)))
		}
	}

	return nil
}
