package storage

import (
	"log"
	"os"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
	sqliteBlobRepository "github.com/jdillenkofer/pithos/internal/storage/repository/blob/sqlite"
	sqliteBucketRepository "github.com/jdillenkofer/pithos/internal/storage/repository/bucket/sqlite"
	sqliteObjectRepository "github.com/jdillenkofer/pithos/internal/storage/repository/object/sqlite"
	sqliteStorageOutboxEntryRepository "github.com/jdillenkofer/pithos/internal/storage/repository/storageoutboxentry/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestMetadataBlobStorageWithOutbox(t *testing.T) {
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
		log.Fatalf("Could not create SqlBlobStore: %s", err)
	}

	storagePath2, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		log.Fatalf("Could not create temp directory: %s", err)
	}
	db2, err := database.OpenDatabase(storagePath2)
	if err != nil {
		log.Fatal("Couldn't open database")
	}
	defer func() {
		err = db2.Close()
		if err != nil {
			log.Fatalf("Could not close database %s", err)
		}
		err = os.RemoveAll(storagePath2)
		if err != nil {
			log.Fatalf("Could not remove storagePath %s: %s", storagePath2, err)
		}
	}()

	// We need a second db, because only one transaction can run at the same time
	// each storage operation opens a new transaction and since outboxStorage and
	// metadataBlobStorage would open separate transactions, we would deadlock the test.
	// To avoid this each storage type gets its own db.
	// In the future i want to redesign storage implementations to use the already open transaction.
	storageOutboxEntryRepository, err := sqliteStorageOutboxEntryRepository.New(db2)
	if err != nil {
		log.Fatalf("Could not create StorageOutboxEntryRepository: %s", err)

	}
	outboxStorage, err := NewOutboxStorage(db2, metadataBlobStorage, storageOutboxEntryRepository)
	if err != nil {
		log.Fatalf("Could not create OutboxStorage: %s", err)
	}

	content := []byte("OutboxStorage")
	err = StorageTester(outboxStorage, content)
	assert.Nil(t, err)
}
