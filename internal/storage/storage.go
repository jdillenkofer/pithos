package storage

import (
	"context"
	"database/sql"
	"io"
	"log"
	"path/filepath"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/blobstore"
	filesystemBlobStore "github.com/jdillenkofer/pithos/internal/storage/blobstore/filesystem"
	encryptionBlobStoreMiddleware "github.com/jdillenkofer/pithos/internal/storage/blobstore/middlewares/encryption"
	tracingBlobStoreMiddleware "github.com/jdillenkofer/pithos/internal/storage/blobstore/middlewares/tracing"
	outboxBlobStore "github.com/jdillenkofer/pithos/internal/storage/blobstore/outbox"
	sqlBlobStore "github.com/jdillenkofer/pithos/internal/storage/blobstore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
	sqliteBlobRepository "github.com/jdillenkofer/pithos/internal/storage/repository/blob/sqlite"
	sqliteBlobContentRepository "github.com/jdillenkofer/pithos/internal/storage/repository/blobcontent/sqlite"
	sqliteBlobOutboxEntryRepository "github.com/jdillenkofer/pithos/internal/storage/repository/bloboutboxentry/sqlite"
	sqliteBucketRepository "github.com/jdillenkofer/pithos/internal/storage/repository/bucket/sqlite"
	sqliteObjectRepository "github.com/jdillenkofer/pithos/internal/storage/repository/object/sqlite"
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

type UploadPartResult struct {
	ETag string
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
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	CreateBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]Bucket, error)
	HeadBucket(ctx context.Context, bucket string) (*Bucket, error)
	ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error)
	HeadObject(ctx context.Context, bucket string, key string) (*Object, error)
	GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error)
	PutObject(ctx context.Context, bucket string, key string, data io.Reader) error
	DeleteObject(ctx context.Context, bucket string, key string) error
	CreateMultipartUpload(ctx context.Context, bucket string, key string) (*InitiateMultipartUploadResult, error)
	UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, data io.Reader) (*UploadPartResult, error)
	CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error
}

func CreateStorage(storagePath string, db *sql.DB, useFilesystemBlobStore bool, blobStoreEncryptionPassword string, wrapBlobStoreWithOutbox bool) Storage {

	var metadataStore metadata.MetadataStore
	bucketRepository, err := sqliteBucketRepository.New(db)
	if err != nil {
		log.Fatalf("Could not create BucketRepository: %s", err)
	}
	objectRepository, err := sqliteObjectRepository.New(db)
	if err != nil {
		log.Fatalf("Could not create ObjectRepository: %s", err)
	}
	blobRepository, err := sqliteBlobRepository.New(db)
	if err != nil {
		log.Fatalf("Could not create BlobRepository: %s", err)
	}
	metadataStore, err = metadata.NewSqlMetadataStore(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		log.Fatal("Error during NewSqlMetadataStore: ", err)
	}

	metadataStore, err = metadata.NewTracingMetadataStoreMiddleware("SqlMetadataStore", metadataStore)
	if err != nil {
		log.Fatal("Error during NewTracingMetadataStoreMiddleware: ", err)
	}

	var blobStore blobstore.BlobStore
	if useFilesystemBlobStore {
		blobStore, err = filesystemBlobStore.New(filepath.Join(storagePath, "blobs"))
		if err != nil {
			log.Fatal("Error during NewFilesystemBlobStore: ", err)
		}
		blobStore, err = tracingBlobStoreMiddleware.New("FilesystemBlobStore", blobStore)
		if err != nil {
			log.Fatal("Error during NewTracingBlobStoreMiddleware: ", err)
		}
	} else {
		blobContentRepository, err := sqliteBlobContentRepository.New(db)
		if err != nil {
			log.Fatalf("Could not create BlobContentRepository: %s", err)
		}
		blobStore, err = sqlBlobStore.New(db, blobContentRepository)
		if err != nil {
			log.Fatal("Error during NewSqlBlobStore: ", err)
		}
		blobStore, err = tracingBlobStoreMiddleware.New("SqlBlobStore", blobStore)
		if err != nil {
			log.Fatal("Error during NewTracingBlobStoreMiddleware: ", err)
		}
	}
	if blobStoreEncryptionPassword != "" {
		blobStore, err = encryptionBlobStoreMiddleware.New(blobStoreEncryptionPassword, blobStore)
		if err != nil {
			log.Fatal("Error during NewEncryptionBlobStoreMiddleware: ", err)
		}
		blobStore, err = tracingBlobStoreMiddleware.New("EncryptionBlobStoreMiddleware", blobStore)
		if err != nil {
			log.Fatal("Error during NewTracingBlobStoreMiddleware: ", err)
		}
	}
	if wrapBlobStoreWithOutbox {
		blobOutboxEntryRepository, err := sqliteBlobOutboxEntryRepository.New(db)
		if err != nil {
			log.Fatalf("Could not create BlobOutboxEntryRepository: %s", err)
		}
		blobStore, err = outboxBlobStore.New(db, blobStore, blobOutboxEntryRepository)
		if err != nil {
			log.Fatal("Error during NewOutboxBlobStore: ", err)
		}
		blobStore, err = tracingBlobStoreMiddleware.New("OutboxBlobStore", blobStore)
		if err != nil {
			log.Fatal("Error during NewTracingBlobStoreMiddleware: ", err)
		}
	}
	var store Storage
	store, err = NewMetadataBlobStorage(db, metadataStore, blobStore)
	if err != nil {
		log.Fatal("Error during NewMetadataBlobStorage: ", err)
	}

	store, err = NewTracingStorageMiddleware("MetadataBlobStorage", store)
	if err != nil {
		log.Fatal("Error during NewTracingStorageMiddleware: ", err)
	}

	return store
}
