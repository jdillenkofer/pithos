package storagefactory

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	tinkEncryptionPartStoreMiddleware "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/middlewares/encryption/tink"
	outboxPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/outbox"
	sqlPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
)

// EncryptionType represents the type of encryption to use for part storage
type EncryptionType string

const (
	EncryptionTypeNone EncryptionType = "none"
	EncryptionTypeTink EncryptionType = "tink"
)

func CreateStorage(storagePath string, db database.Database, useFilesystemPartStore bool, encryptionType EncryptionType, partStoreEncryptionPassword string, wrapPartStoreWithOutbox bool) storage.Storage {
	var metadataStore metadatastore.MetadataStore
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
	metadataStore, err = sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository)
	if err != nil {
		slog.Error(fmt.Sprint("Error during NewSqlMetadataStore: ", err))
		os.Exit(1)
	}

	var partStore partstore.PartStore
	if useFilesystemPartStore {
		partStore, err = filesystemPartStore.New(filepath.Join(storagePath, "parts"))
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewFilesystemPartStore: ", err))
			os.Exit(1)
		}
	} else {
		partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not create PartContentRepository: %s", err))
			os.Exit(1)
		}
		partStore, err = sqlPartStore.New(db, partContentRepository)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewSqlPartStore: ", err))
			os.Exit(1)
		}
	}

	// Apply encryption middleware based on encryption type
	switch encryptionType {
	case EncryptionTypeTink:
		if partStoreEncryptionPassword == "" {
			slog.Error("Encryption password is required for Tink encryption")
			os.Exit(1)
		}
		partStore, err = tinkEncryptionPartStoreMiddleware.NewWithLocalKMS(partStoreEncryptionPassword, partStore, nil)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewTinkEncryptionPartStoreMiddleware: ", err))
			os.Exit(1)
		}
	case EncryptionTypeNone:
		// No encryption
	default:
		slog.Error(fmt.Sprintf("Unknown encryption type: %s", encryptionType))
		os.Exit(1)
	}

	if wrapPartStoreWithOutbox {
		partOutboxEntryRepository, err := repositoryFactory.NewPartOutboxEntryRepository(db)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not create PartOutboxEntryRepository: %s", err))
			os.Exit(1)
		}
		partStore, err = outboxPartStore.New(db, partStore, partOutboxEntryRepository)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewOutboxPartStore: ", err))
			os.Exit(1)
		}
	}
	var store storage.Storage
	store, err = metadatapart.NewStorage(db, metadataStore, partStore)
	if err != nil {
		slog.Error(fmt.Sprint("Error during NewMetadataPartStorage: ", err))
		os.Exit(1)
	}

	return store
}
