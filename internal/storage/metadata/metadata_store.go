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
	BlobIds      []blob.BlobId
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
	ListObjects(bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) ([]Object, []string, error)
	HeadObject(bucketName string, key string) (*Object, error)
	PutObject(bucketName string, object *Object) error
	DeleteObject(bucketName string, key string) error
	Clear() error
}
