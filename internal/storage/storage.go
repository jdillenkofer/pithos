package storage

import (
	"context"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
)

type Bucket struct {
	Name         BucketName
	CreationDate time.Time
}

type Object struct {
	Key               ObjectKey
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

// ByteRange represents a byte range for GetObject operations.
// Start and End use exclusive end indexing (End points one past the last byte).
// Examples:
//   - Start=0, End=10: bytes 0-9 (10 bytes)
//   - Start=nil, End=nil: entire object
//   - Start=nil, End=500: suffix range - last 500 bytes (will be converted to absolute range)
//
// Suffix ranges (Start=nil, End=N) are converted to absolute ranges by the storage layer.
type ByteRange struct {
	Start *int64
	End   *int64
}

type ListBucketResult struct {
	Objects        []Object
	CommonPrefixes []string
	IsTruncated    bool
}

type PutObjectResult struct {
	ETag              *string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
}

type InitiateMultipartUploadResult struct {
	UploadId UploadId
}

type UploadPartResult struct {
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
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
	Key       ObjectKey
	UploadId  UploadId
	Initiated time.Time
}

type ListMultipartUploadsResult struct {
	BucketName         BucketName
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
	BucketName           BucketName
	Key                  ObjectKey
	UploadId             UploadId
	PartNumberMarker     string
	NextPartNumberMarker *string
	MaxParts             int32
	IsTruncated          bool
	Parts                []*Part
}

type ChecksumInput = metadatastore.ChecksumInput
type ChecksumValues = metadatastore.ChecksumValues
type BucketName = metadatastore.BucketName
type ObjectKey = metadatastore.ObjectKey
type UploadId = metadatastore.UploadId

var NewBucketName = metadatastore.NewBucketName
var MustNewBucketName = metadatastore.MustNewBucketName

var NewObjectKey = metadatastore.NewObjectKey
var MustNewObjectKey = metadatastore.MustNewObjectKey

var NewUploadId = metadatastore.NewUploadId
var MustNewUploadId = metadatastore.MustNewUploadId

var ValidateChecksums = metadatastore.ValidateChecksums

var ErrNoSuchBucket error = metadatastore.ErrNoSuchBucket
var ErrBucketAlreadyExists error = metadatastore.ErrBucketAlreadyExists
var ErrBucketNotEmpty error = metadatastore.ErrBucketNotEmpty
var ErrNoSuchKey error = metadatastore.ErrNoSuchKey
var ErrBadDigest error = metadatastore.ErrBadDigest
var ErrNotImplemented error = metadatastore.ErrNotImplemented
var ErrEntityTooLarge error = metadatastore.ErrEntityTooLarge
var ErrInvalidBucketName error = metadatastore.ErrInvalidBucketName
var ErrInvalidObjectKey error = metadatastore.ErrInvalidObjectKey
var ErrInvalidUploadId error = metadatastore.ErrInvalidUploadId
var ErrInvalidRange error = errors.New("InvalidRange")

var MaxEntitySize int64 = 900 * 1000 * 1000 // 900 MB

// ListObjectsOptions defines options for listing objects
type ListObjectsOptions struct {
	// Prefix limits the response to keys that begin with the specified prefix.
	Prefix *string
	// Delimiter is a character you use to group keys.
	Delimiter *string
	// StartAfter is where you want Amazon S3 to start the listing from. Amazon S3 starts listing after this specified key.
	StartAfter *string
	// MaxKeys sets the maximum number of keys returned in the response.
	MaxKeys int32
}

// ListMultipartUploadsOptions defines options for listing multipart uploads
type ListMultipartUploadsOptions struct {
	// Prefix limits the response to keys that begin with the specified prefix.
	Prefix *string
	// Delimiter is a character you use to group keys.
	Delimiter *string
	// KeyMarker specifies the key-marker query parameter in the request.
	KeyMarker *string
	// UploadIdMarker specifies the upload-id-marker query parameter in the request.
	UploadIdMarker *string
	// MaxUploads sets the maximum number of multipart uploads returned in the response.
	MaxUploads int32
}

// ListPartsOptions defines options for listing parts
type ListPartsOptions struct {
	// PartNumberMarker specifies the part-number-marker query parameter in the request.
	PartNumberMarker *string
	// MaxParts sets the maximum number of parts returned in the response.
	MaxParts int32
}

// BucketManager manages bucket operations
type BucketManager interface {
	CreateBucket(ctx context.Context, bucketName BucketName) error
	DeleteBucket(ctx context.Context, bucketName BucketName) error
	ListBuckets(ctx context.Context) ([]Bucket, error)
	HeadBucket(ctx context.Context, bucketName BucketName) (*Bucket, error)
}

// ObjectManager manages object operations
type ObjectManager interface {
	ListObjects(ctx context.Context, bucketName BucketName, opts ListObjectsOptions) (*ListBucketResult, error)
	HeadObject(ctx context.Context, bucketName BucketName, key ObjectKey) (*Object, error)
	// GetObject retrieves an object with optional byte ranges.
	// If ranges is empty or nil, returns the entire object as a single reader.
	// All operations are performed in a single transaction to ensure consistency.
	// Returns the object metadata, a list of readers (one per range), and an error.
	GetObject(ctx context.Context, bucketName BucketName, key ObjectKey, ranges []ByteRange) (*Object, []io.ReadCloser, error)
	PutObject(ctx context.Context, bucketName BucketName, key ObjectKey, contentType *string, data io.Reader, checksumInput *ChecksumInput) (*PutObjectResult, error)
	DeleteObject(ctx context.Context, bucketName BucketName, key ObjectKey) error
}

// MultipartUploadManager manages multipart upload operations
type MultipartUploadManager interface {
	CreateMultipartUpload(ctx context.Context, bucketName BucketName, key ObjectKey, contentType *string, checksumType *string) (*InitiateMultipartUploadResult, error)
	UploadPart(ctx context.Context, bucketName BucketName, key ObjectKey, uploadId UploadId, partNumber int32, data io.Reader, checksumInput *ChecksumInput) (*UploadPartResult, error)
	CompleteMultipartUpload(ctx context.Context, bucketName BucketName, key ObjectKey, uploadId UploadId, checksumInput *ChecksumInput) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, bucketName BucketName, key ObjectKey, uploadId UploadId) error
	ListMultipartUploads(ctx context.Context, bucketName BucketName, opts ListMultipartUploadsOptions) (*ListMultipartUploadsResult, error)
	ListParts(ctx context.Context, bucketName BucketName, key ObjectKey, uploadId UploadId, opts ListPartsOptions) (*ListPartsResult, error)
}

// Storage is a composite interface that combines all storage operations
type Storage interface {
	lifecycle.Manager
	BucketManager
	ObjectManager
	MultipartUploadManager
}

func ListAllObjectsOfBucket(ctx context.Context, storage Storage, bucketName BucketName) ([]Object, error) {
	allObjects := []Object{}
	var startAfter *string
	for {
		listBucketResult, err := storage.ListObjects(ctx, bucketName, ListObjectsOptions{
			StartAfter: startAfter,
			MaxKeys:    1000,
		})
		if err != nil {
			return nil, err
		}
		allObjects = append(allObjects, listBucketResult.Objects...)
		if !listBucketResult.IsTruncated {
			break
		}
		startAfter = ptrutils.ToPtr(listBucketResult.Objects[len(listBucketResult.Objects)-1].Key.String())
	}
	return allObjects, nil
}

func Tester(storage Storage, bucketNames []BucketName, content []byte) error {
	ctx := context.Background()
	err := storage.Start(ctx)
	if err != nil {
		return err
	}
	defer storage.Stop(ctx)

	for _, bucketName := range bucketNames {
		key := MustNewObjectKey("test")
		data := ioutils.NewByteReadSeekCloser(content)

		err = storage.CreateBucket(ctx, bucketName)
		if err != nil {
			return err
		}

		bucket, err := storage.HeadBucket(ctx, bucketName)
		if err != nil {
			return err
		}

		if !bucketName.Equals(bucket.Name) {
			return errors.New("invalid bucketName")
		}

		buckets, err := storage.ListBuckets(ctx)
		if err != nil {
			return err
		}

		if len(buckets) != 1 {
			return errors.New("expected 1 bucket got " + strconv.Itoa(len(buckets)))
		}

		if !bucketName.Equals(buckets[0].Name) {
			return errors.New("invalid bucketName")
		}

		_, err = storage.PutObject(ctx, bucketName, key, nil, data, nil)
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

		listBucketResult, err := storage.ListObjects(ctx, bucketName, ListObjectsOptions{
			MaxKeys: 1000,
		})
		if err != nil {
			return err
		}

		if len(listBucketResult.Objects) != 1 {
			return errors.New("invalid objects length")
		}

		if !key.Equals(listBucketResult.Objects[0].Key) {
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

		_, err = storage.UploadPart(ctx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, data, nil)
		if err != nil {
			return err
		}

		_, err = storage.CompleteMultipartUpload(ctx, bucketName, key, initiateMultipartUploadResult.UploadId, nil)
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

		_, err = storage.UploadPart(ctx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, data, nil)
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
