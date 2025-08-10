package outbox

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlob "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/blob"
	sqliteBucket "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/bucket"
	sqliteObject "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/object"
	sqliteStorageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/storageoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob"
	filesystemBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/filesystem"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/sql"
	"github.com/stretchr/testify/assert"
)

func TestMetadataBlobStorageWithOutbox(t *testing.T) {
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := database.OpenDatabase(dbPath)
	if err != nil {
		slog.Error("Couldn't open database")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()

	blobStore, err := filesystemBlobStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlBlobStore: %s", err))
		os.Exit(1)
	}

	bucketRepository, err := sqliteBucket.NewRepository()
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BucketRepository: %s", err))
		os.Exit(1)
	}
	objectRepository, err := sqliteObject.NewRepository()
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ObjectRepository: %s", err))
		os.Exit(1)
	}
	blobRepository, err := sqliteBlob.NewRepository()
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BlobRepository: %s", err))
		os.Exit(1)
	}
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlMetadataStore: %s", err))
		os.Exit(1)
	}

	metadataBlobStorage, err := metadatablob.NewStorage(db, metadataStore, blobStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlBlobStore: %s", err))
		os.Exit(1)
	}

	storagePath2, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath2 := filepath.Join(storagePath2, "pithos.db")
	db2, err := database.OpenDatabase(dbPath2)
	if err != nil {
		slog.Error("Couldn't open database")
		os.Exit(1)
	}
	defer func() {
		err = db2.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath2)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath2, err))
			os.Exit(1)
		}
	}()

	// We need a second db, because only one transaction can run at the same time
	// each storage operation opens a new transaction and since outboxStorage and
	// metadataBlobStorage would open separate transactions, we would deadlock the test.
	// To avoid this each storage type gets its own db.
	// In the future i want to redesign storage implementations to use the already open transaction.
	storageOutboxEntryRepository, err := sqliteStorageOutboxEntry.NewRepository()
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create StorageOutboxEntryRepository: %s", err))
		os.Exit(1)

	}
	outboxStorage, err := NewStorage(db2, metadataBlobStorage, storageOutboxEntryRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create OutboxStorage: %s", err))
		os.Exit(1)
	}

	content := []byte("OutboxStorage")
	err = storage.Tester(outboxStorage, []string{"bucket"}, content)
	assert.Nil(t, err)
}
