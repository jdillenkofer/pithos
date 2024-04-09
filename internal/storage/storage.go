package storage

import (
	"io"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadata"
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
}

var ErrNoSuchBucket error = metadata.ErrNoSuchBucket
var ErrBucketAlreadyExists error = metadata.ErrBucketAlreadyExists
var ErrBucketNotEmpty error = metadata.ErrBucketNotEmpty
var ErrNoSuchKey error = metadata.ErrNoSuchKey

type Storage interface {
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	ListBuckets() ([]Bucket, error)
	HeadBucket(bucket string) (*Bucket, error)
	ListObjects(bucket string, prefix string, delimiter string, startAfter string, maxKeys int) ([]Object, []string, error)
	HeadObject(bucket string, key string) (*Object, error)
	GetObject(bucket string, key string) (io.ReadCloser, error)
	PutObject(bucket string, key string, data io.Reader) error
	DeleteObject(bucket string, key string) error
	Clear() error
}
