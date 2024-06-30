package storage

import (
	"database/sql"
	"io"
	"log"
	"path/filepath"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/blob"
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

type ListBucketResult struct {
	Objects        []Object
	CommonPrefixes []string
	IsTruncated    bool
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

var ErrNoSuchBucket error = metadata.ErrNoSuchBucket
var ErrBucketAlreadyExists error = metadata.ErrBucketAlreadyExists
var ErrBucketNotEmpty error = metadata.ErrBucketNotEmpty
var ErrNoSuchKey error = metadata.ErrNoSuchKey

type Storage interface {
	Start() error
	Stop() error
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	ListBuckets() ([]Bucket, error)
	HeadBucket(bucket string) (*Bucket, error)
	ListObjects(bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error)
	HeadObject(bucket string, key string) (*Object, error)
	GetObject(bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error)
	PutObject(bucket string, key string, data io.Reader) error
	DeleteObject(bucket string, key string) error
	CreateMultipartUpload(bucket string, key string) (*InitiateMultipartUploadResult, error)
	UploadPart(bucket string, key string, uploadId string, partNumber int32, data io.Reader) error
	CompleteMultipartUpload(bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(bucket string, key string, uploadId string) error
}

func CreateAndInitializeStorage(storagePath string, db *sql.DB, useFilesystemBlobStore bool, wrapBlobStoreWithOutbox bool) Storage {
	metadataStore, err := metadata.NewSqlMetadataStore()
	if err != nil {
		log.Fatal("Error during NewSqlMetadataStore: ", err)
	}
	var blobStore blob.BlobStore
	if useFilesystemBlobStore {
		blobStore, err = blob.NewFilesystemBlobStore(filepath.Join(storagePath, "blobs"))
		if err != nil {
			log.Fatal("Error during NewFilesystemBlobStore: ", err)
		}
	} else {
		blobStore, err = blob.NewSqlBlobStore()
		if err != nil {
			log.Fatal("Error during NewSqlBlobStore: ", err)
		}
	}
	if wrapBlobStoreWithOutbox {
		blobStore, err = blob.NewOutboxBlobStore(db, blobStore)
		if err != nil {
			log.Fatal("Error during NewOutboxBlobStore: ", err)
		}
	}
	var storage Storage
	storage, err = NewMetadataBlobStorage(db, metadataStore, blobStore)
	if err != nil {
		log.Fatal("Error during NewMetadataBlobStorage: ", err)
	}

	err = storage.Start()
	if err != nil {
		log.Fatal("Error during storage initialization: ", err)
	}
	return storage
}
