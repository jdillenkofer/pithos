package storage

import (
	"bytes"
	"io"
	"strings"
	"time"
)

type MockStorage struct {
}

var mockBucket Bucket = Bucket{
	Name:         "mock-bucket",
	CreationDate: time.Now(),
}

var mockObject Object = Object{
	Key:          "object.txt",
	LastModified: time.Now(),
	ETag:         "abcdef",
	Size:         100,
}

func NewMockStorage() (*MockStorage, error) {
	return &MockStorage{}, nil
}

func (*MockStorage) CreateBucket(bucket string) error {
	return nil
}

func (*MockStorage) DeleteBucket(bucket string) error {
	return nil
}

func (*MockStorage) ListBuckets() ([]Bucket, error) {
	return []Bucket{mockBucket}, nil
}

func (*MockStorage) ExistBucket(bucket string) (*Bucket, error) {
	if mockBucket.Name != bucket {
		return nil, ErrBucketNotFound
	}
	return &mockBucket, nil
}

func (*MockStorage) ListObjects(bucket string) ([]Object, error) {
	if mockBucket.Name != bucket {
		return nil, ErrBucketNotFound
	}
	return []Object{mockObject}, nil
}

func (*MockStorage) ExistObject(bucket string, key string) (*Object, error) {
	if mockBucket.Name != bucket {
		return nil, ErrBucketNotFound
	}
	return &mockObject, nil
}

func (*MockStorage) GetObject(bucket string, key string) (io.Reader, error) {
	if mockBucket.Name != bucket {
		return nil, ErrBucketNotFound
	}
	return bytes.NewReader([]byte(strings.Repeat("a", int(mockObject.Size)))), nil
}

func (*MockStorage) PutObject(bucket string, key string, data io.Reader) error {
	if mockBucket.Name != bucket {
		return ErrBucketNotFound
	}
	return nil
}

func (*MockStorage) DeleteObject(bucket string, key string) error {
	if mockBucket.Name != bucket {
		return ErrBucketNotFound
	}
	return nil
}
