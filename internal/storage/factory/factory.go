package storagefactory

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	filesystemBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/filesystem"
	legacyEncryptionBlobStoreMiddleware "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/encryption/legacy"
	tinkEncryptionBlobStoreMiddleware "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/encryption/tink"
	outboxBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/outbox"
	sqlBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/sql"
)

// EncryptionType represents the type of encryption to use for blob storage
type EncryptionType string

const (
	EncryptionTypeNone   EncryptionType = "none"
	EncryptionTypeLegacy EncryptionType = "legacy"
	EncryptionTypeTink   EncryptionType = "tink"
)

func CreateStorage(storagePath string, db database.Database, useFilesystemBlobStore bool, encryptionType EncryptionType, blobStoreEncryptionPassword string, wrapBlobStoreWithOutbox bool) storage.Storage {
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
	blobRepository, err := repositoryFactory.NewBlobRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BlobRepository: %s", err))
		os.Exit(1)
	}
	metadataStore, err = sqlMetadataStore.New(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		slog.Error(fmt.Sprint("Error during NewSqlMetadataStore: ", err))
		os.Exit(1)
	}

	var blobStore blobstore.BlobStore
	if useFilesystemBlobStore {
		blobStore, err = filesystemBlobStore.New(filepath.Join(storagePath, "blobs"))
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewFilesystemBlobStore: ", err))
			os.Exit(1)
		}
	} else {
		blobContentRepository, err := repositoryFactory.NewBlobContentRepository(db)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not create BlobContentRepository: %s", err))
			os.Exit(1)
		}
		blobStore, err = sqlBlobStore.New(db, blobContentRepository)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewSqlBlobStore: ", err))
			os.Exit(1)
		}
	}

	// Apply encryption middleware based on encryption type
	switch encryptionType {
	case EncryptionTypeLegacy:
		if blobStoreEncryptionPassword == "" {
			slog.Error("Encryption password is required for legacy encryption")
			os.Exit(1)
		}
		blobStore, err = legacyEncryptionBlobStoreMiddleware.New(blobStoreEncryptionPassword, blobStore)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewEncryptionBlobStoreMiddleware: ", err))
			os.Exit(1)
		}
	case EncryptionTypeTink:
		if blobStoreEncryptionPassword == "" {
			slog.Error("Encryption password is required for Tink encryption")
			os.Exit(1)
		}
		blobStore, err = tinkEncryptionBlobStoreMiddleware.NewWithLocalKMS(blobStoreEncryptionPassword, blobStore)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewTinkEncryptionBlobStoreMiddleware: ", err))
			os.Exit(1)
		}
	case EncryptionTypeNone:
		// No encryption
	default:
		slog.Error(fmt.Sprintf("Unknown encryption type: %s", encryptionType))
		os.Exit(1)
	}

	if wrapBlobStoreWithOutbox {
		blobOutboxEntryRepository, err := repositoryFactory.NewBlobOutboxEntryRepository(db)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not create BlobOutboxEntryRepository: %s", err))
			os.Exit(1)
		}
		blobStore, err = outboxBlobStore.New(db, blobStore, blobOutboxEntryRepository)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewOutboxBlobStore: ", err))
			os.Exit(1)
		}
	}
	var store storage.Storage
	store, err = metadatablob.NewStorage(db, metadataStore, blobStore)
	if err != nil {
		slog.Error(fmt.Sprint("Error during NewMetadataBlobStorage: ", err))
		os.Exit(1)
	}

	return store
}
