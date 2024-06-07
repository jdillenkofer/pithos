package storage

import (
	"database/sql"
	"io"
	"log"
	"os"
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
}

func CreateAndInitializeStorage(storagePath string, useFilesystemBlobStore bool, wrapBlobStoreWithOutbox bool) (storage Storage, closeStorage func()) {
	err := os.MkdirAll(storagePath, os.ModePerm)
	if err != nil {
		log.Fatal("Error while creating data directory: ", err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(storagePath, "pithos.db"))
	if err != nil {
		log.Fatal("Error when opening sqlite database: ", err)
	}
	err = SetupDatabase(db)
	if err != nil {
		log.Fatal("Error during SetupDatabase: ", err)
	}
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
	storage, err = NewMetadataBlobStorage(db, metadataStore, blobStore)
	if err != nil {
		log.Fatal("Error during NewMetadataBlobStorage: ", err)
	}

	err = storage.Start()
	if err != nil {
		log.Fatal("Error during storage initialization: ", err)
	}
	closeStorage = func() {
		err := storage.Stop()
		if err != nil {
			log.Fatal("Error during storage closing: ", err)
		}
		err = db.Close()
		if err != nil {
			log.Fatal("Error when closing db: ", err)
		}
	}
	return
}
