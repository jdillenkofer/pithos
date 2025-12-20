package outbox

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestMetadataPartStorageWithOutbox(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		slog.Error(fmt.Sprintf("Couldn't open database: %v", err))
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

	partStore, err := filesystemPartStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlPartStore: %s", err))
		os.Exit(1)
	}

	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BucketRepository: %s", err))
		os.Exit(1)
	}
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ObjectRepository: %s", err))
		os.Exit(1)
	}
	partRepository, err := repositoryFactory.NewPartRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartRepository: %s", err))
		os.Exit(1)
	}
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlMetadataStore: %s", err))
		os.Exit(1)
	}

	metadataPartStorage, err := metadatapart.NewStorage(db, metadataStore, partStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlPartStore: %s", err))
		os.Exit(1)
	}

	storagePath2, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath2 := filepath.Join(storagePath2, "pithos.db")
	db2, err := sqlite.OpenDatabase(dbPath2)
	if err != nil {
		slog.Error(fmt.Sprintf("Couldn't open database 2: %v", err))
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
	// metadataPartStorage would open separate transactions, we would deadlock the test.
	// To avoid this each storage type gets its own db.
	// In the future i want to redesign storage implementations to use the already open transaction.
	storageOutboxEntryRepository, err := repositoryFactory.NewStorageOutboxEntryRepository(db2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create StorageOutboxEntryRepository: %s", err))
		os.Exit(1)

	}
	outboxStorage, err := NewStorage(db2, metadataPartStorage, storageOutboxEntryRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create OutboxStorage: %s", err))
		os.Exit(1)
	}

	content := []byte("OutboxStorage")
	err = storage.Tester(outboxStorage, []storage.BucketName{storage.MustNewBucketName("bucket")}, content)
	assert.Nil(t, err)
}
