package metadata

import (
	"errors"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/blob"
)

type Bucket struct {
	Name         string
	CreationDate time.Time
}

type Object struct {
	Key          string
	LastModified time.Time
	ETag         string
	Size         int64
	Blobs        []Blob
}

type ListBucketResult struct {
	Objects        []Object
	CommonPrefixes []string
	IsTruncated    bool
}

type Blob struct {
	Id   blob.BlobId
	Size int64
	ETag string
}

type InitiateMultipartUploadResult struct {
	UploadId string
}

type CompleteMultipartUploadResult struct {
	Location       string
	ETag           string
	ChecksumCRC32  string
	ChecksumCRC32C string
	ChecksumSHA1   string
	ChecksumSHA256 string
}

type AbortMultipartResult struct {
	Blobs []Blob
}

var ErrNoSuchBucket error = errors.New("NoSuchBucket")
var ErrBucketAlreadyExists error = errors.New("BucketAlreadyExists")
var ErrBucketNotEmpty error = errors.New("BucketNotEmpty")
var ErrNoSuchKey error = errors.New("NoSuchKey")

type MetadataStore interface {
	CreateBucket(bucketName string) error
	DeleteBucket(bucketName string) error
	ListBuckets() ([]Bucket, error)
	HeadBucket(bucketName string) (*Bucket, error)
	ListObjects(bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error)
	HeadObject(bucketName string, key string) (*Object, error)
	PutObject(bucketName string, object *Object) error
	DeleteObject(bucketName string, key string) error
	CreateMultipartUpload(bucketName string, key string) (*InitiateMultipartUploadResult, error)
	UploadPart(bucketName string, key string, uploadId string, partNumber uint16, blob Blob) error
	CompleteMultipartUpload(bucketName string, key string, uploadId string) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(bucketName string, key string, uploadId string) (*AbortMultipartResult, error)
}
