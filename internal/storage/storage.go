package storage

import (
	"errors"
	"io"
	"time"
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

var ErrBucketNotFound error = errors.New("Bucket not found")
var ErrBucketAlreadyExists error = errors.New("Bucket already exists")
var ErrBucketNotEmpty error = errors.New("Bucket not empty")

type Storage interface {
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	ListBuckets() ([]Bucket, error)
	ExistBucket(bucket string) (*Bucket, error)
	ListObjects(bucket string) ([]Object, error)
	ExistObject(bucket string, key string) (*Object, error)
	GetObject(bucket string, key string) (io.ReadCloser, error)
	PutObject(bucket string, key string, data io.Reader) error
	DeleteObject(bucket string, key string) error
}
