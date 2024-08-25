package metadata

import (
	"context"
	"database/sql"
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
	DeletedBlobs   []Blob
	Location       string
	ETag           string
	ChecksumCRC32  string
	ChecksumCRC32C string
	ChecksumSHA1   string
	ChecksumSHA256 string
}

type AbortMultipartResult struct {
	DeletedBlobs []Blob
}

var ErrNoSuchBucket error = errors.New("NoSuchBucket")
var ErrBucketAlreadyExists error = errors.New("BucketAlreadyExists")
var ErrBucketNotEmpty error = errors.New("BucketNotEmpty")
var ErrNoSuchKey error = errors.New("NoSuchKey")
var ErrUploadWithInvalidSequenceNumber error = errors.New("UploadWithInvalidSequenceNumber")

type MetadataStore interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	CreateBucket(ctx context.Context, tx *sql.Tx, bucketName string) error
	DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName string) error
	ListBuckets(ctx context.Context, tx *sql.Tx) ([]Bucket, error)
	HeadBucket(ctx context.Context, tx *sql.Tx, bucketName string) (*Bucket, error)
	ListObjects(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error)
	HeadObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*Object, error)
	PutObject(ctx context.Context, tx *sql.Tx, bucketName string, object *Object) error
	DeleteObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) error
	CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*InitiateMultipartUploadResult, error)
	UploadPart(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string, partNumber int32, blob Blob) error
	CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*AbortMultipartResult, error)
}
