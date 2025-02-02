package outbox

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlob "github.com/jdillenkofer/pithos/internal/storage/database/repository/blob/sqlite"
	sqliteBucket "github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket/sqlite"
	sqliteObject "github.com/jdillenkofer/pithos/internal/storage/database/repository/object/sqlite"
	sqliteStorageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob"
	filesystemBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/filesystem"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/sql"
	"github.com/stretchr/testify/assert"
)

func TestMetadataBlobStorageWithOutbox(t *testing.T) {
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		log.Fatalf("Could not create temp directory: %s", err)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := database.OpenDatabase(dbPath)
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

	blobStore, err := filesystemBlobStore.New(storagePath)
	if err != nil {
		log.Fatalf("Could not create SqlBlobStore: %s", err)
	}

	bucketRepository, err := sqliteBucket.NewRepository()
	if err != nil {
		log.Fatalf("Could not create BucketRepository: %s", err)
	}
	objectRepository, err := sqliteObject.NewRepository()
	if err != nil {
		log.Fatalf("Could not create ObjectRepository: %s", err)
	}
	blobRepository, err := sqliteBlob.NewRepository()
	if err != nil {
		log.Fatalf("Could not create BlobRepository: %s", err)
	}
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		log.Fatalf("Could not create SqlMetadataStore: %s", err)
	}

	metadataBlobStorage, err := metadatablob.NewStorage(db, metadataStore, blobStore)
	if err != nil {
		log.Fatalf("Could not create SqlBlobStore: %s", err)
	}

	storagePath2, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		log.Fatalf("Could not create temp directory: %s", err)
	}
	dbPath2 := filepath.Join(storagePath2, "pithos.db")
	db2, err := database.OpenDatabase(dbPath2)
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
	storageOutboxEntryRepository, err := sqliteStorageOutboxEntry.NewRepository()
	if err != nil {
		log.Fatalf("Could not create StorageOutboxEntryRepository: %s", err)

	}
	outboxStorage, err := NewStorage(db2, metadataBlobStorage, storageOutboxEntryRepository)
	if err != nil {
		log.Fatalf("Could not create OutboxStorage: %s", err)
	}

	content := []byte("OutboxStorage")
	err = storage.Tester(outboxStorage, []string{"bucket"}, content)
	assert.Nil(t, err)
}
