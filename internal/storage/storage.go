package storage

import (
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

type Storage interface {
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	ListBuckets() ([]Bucket, error)
	ExistBucket(bucket string) (*Bucket, error)
	ListObjects(bucket string) ([]Object, error)
	ExistObject(bucket string, key string) (*Object, error)
	GetObject(bucket string, key string) (io.Reader, error)
	PutObject(bucket string, key string, data io.Reader) error
	DeleteObject(bucket string, key string) error
}
