package storage

import (
	"log"
	"os"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
	sqliteBlobRepository "github.com/jdillenkofer/pithos/internal/storage/repository/blob/sqlite"
	sqliteBlobContentRepository "github.com/jdillenkofer/pithos/internal/storage/repository/blobcontent/sqlite"
	sqliteBucketRepository "github.com/jdillenkofer/pithos/internal/storage/repository/bucket/sqlite"
	sqliteObjectRepository "github.com/jdillenkofer/pithos/internal/storage/repository/object/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestMetadataBlobStorageWithSql(t *testing.T) {
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		log.Fatalf("Could not create temp directory: %s", err)
	}
	db, err := database.OpenDatabase(storagePath)
	if err != nil {
		log.Fatal("Couldn't open database")
	}
	defer func() {
		err = db.Close()
		if err != nil {
			log.Fatalf("Could not close database %s", err)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			log.Fatalf("Could not remove storagePath %s: %s", storagePath, err)
		}
	}()

	blobContentRepository, err := sqliteBlobContentRepository.New(db)
	if err != nil {
		log.Fatalf("Could not create BlobContentRepository: %s", err)
	}
	blobStore, err := blob.NewSqlBlobStore(db, blobContentRepository)
	if err != nil {
		log.Fatalf("Could not create SqlBlobStore: %s", err)
	}

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
	metadataStore, err := metadata.NewSqlMetadataStore(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		log.Fatalf("Could not create SqlMetadataStore: %s", err)
	}

	metadataBlobStorage, err := NewMetadataBlobStorage(db, metadataStore, blobStore)
	if err != nil {
		log.Fatalf("Could not create MetadataBlobStorage: %s", err)
	}
	content := []byte("MetadataBlobStorage")
	err = StorageTester(metadataBlobStorage, content)
	assert.Nil(t, err)
}

func TestMetadataBlobStorageWithFilesystem(t *testing.T) {
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		log.Fatalf("Could not create temp directory: %s", err)
	}
	db, err := database.OpenDatabase(storagePath)
	if err != nil {
		log.Fatal("Couldn't open database")
	}
	defer func() {
		err = db.Close()
		if err != nil {
			log.Fatalf("Could not close database %s", err)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			log.Fatalf("Could not remove storagePath %s: %s", storagePath, err)
		}
	}()

	blobStore, err := blob.NewFilesystemBlobStore(storagePath)
	if err != nil {
		log.Fatalf("Could not create SqlBlobStore: %s", err)
	}

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
	metadataStore, err := metadata.NewSqlMetadataStore(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		log.Fatalf("Could not create SqlMetadataStore: %s", err)
	}

	metadataBlobStorage, err := NewMetadataBlobStorage(db, metadataStore, blobStore)
	if err != nil {
		log.Fatalf("Could not create MetadataBlobStorage: %s", err)
	}
	content := []byte("MetadataBlobStorage")
	err = StorageTester(metadataBlobStorage, content)
	assert.Nil(t, err)
}
