package storagefactory

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlob "github.com/jdillenkofer/pithos/internal/storage/database/repository/blob/sqlite"
	sqliteBlobContent "github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent/sqlite"
	sqliteBlobOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/bloboutboxentry/sqlite"
	sqliteBucket "github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket/sqlite"
	sqliteObject "github.com/jdillenkofer/pithos/internal/storage/database/repository/object/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	filesystemBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/filesystem"
	encryptionBlobStoreMiddleware "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/encryption"
	tracingBlobStoreMiddleware "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/tracing"
	outboxBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/outbox"
	sqlBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	tracingMetadataStoreMiddleware "github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/middlewares/tracing"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/sql"
	tracingStorageMiddleware "github.com/jdillenkofer/pithos/internal/storage/middlewares/tracing"
)

func CreateStorage(storagePath string, db database.Database, useFilesystemBlobStore bool, blobStoreEncryptionPassword string, wrapBlobStoreWithOutbox bool) storage.Storage {
	var metadataStore metadatastore.MetadataStore
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
	metadataStore, err = sqlMetadataStore.New(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		slog.Error(fmt.Sprint("Error during NewSqlMetadataStore: ", err))
		os.Exit(1)
	}

	metadataStore, err = tracingMetadataStoreMiddleware.New("SqlMetadataStore", metadataStore)
	if err != nil {
		slog.Error(fmt.Sprint("Error during NewTracingMetadataStoreMiddleware: ", err))
		os.Exit(1)
	}

	var blobStore blobstore.BlobStore
	if useFilesystemBlobStore {
		blobStore, err = filesystemBlobStore.New(filepath.Join(storagePath, "blobs"))
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewFilesystemBlobStore: ", err))
			os.Exit(1)
		}
		blobStore, err = tracingBlobStoreMiddleware.New("FilesystemBlobStore", blobStore)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewTracingBlobStoreMiddleware: ", err))
			os.Exit(1)
		}
	} else {
		blobContentRepository, err := sqliteBlobContent.NewRepository()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not create BlobContentRepository: %s", err))
			os.Exit(1)
		}
		blobStore, err = sqlBlobStore.New(db, blobContentRepository)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewSqlBlobStore: ", err))
			os.Exit(1)
		}
		blobStore, err = tracingBlobStoreMiddleware.New("SqlBlobStore", blobStore)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewTracingBlobStoreMiddleware: ", err))
			os.Exit(1)
		}
	}
	if blobStoreEncryptionPassword != "" {
		blobStore, err = encryptionBlobStoreMiddleware.New(blobStoreEncryptionPassword, blobStore)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewEncryptionBlobStoreMiddleware: ", err))
			os.Exit(1)
		}
		blobStore, err = tracingBlobStoreMiddleware.New("EncryptionBlobStoreMiddleware", blobStore)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewTracingBlobStoreMiddleware: ", err))
			os.Exit(1)
		}
	}
	if wrapBlobStoreWithOutbox {
		blobOutboxEntryRepository, err := sqliteBlobOutboxEntry.NewRepository()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not create BlobOutboxEntryRepository: %s", err))
			os.Exit(1)
		}
		blobStore, err = outboxBlobStore.New(db, blobStore, blobOutboxEntryRepository)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewOutboxBlobStore: ", err))
			os.Exit(1)
		}
		blobStore, err = tracingBlobStoreMiddleware.New("OutboxBlobStore", blobStore)
		if err != nil {
			slog.Error(fmt.Sprint("Error during NewTracingBlobStoreMiddleware: ", err))
			os.Exit(1)
		}
	}
	var store storage.Storage
	store, err = metadatablob.NewStorage(db, metadataStore, blobStore)
	if err != nil {
		slog.Error(fmt.Sprint("Error during NewMetadataBlobStorage: ", err))
		os.Exit(1)
	}

	store, err = tracingStorageMiddleware.NewStorageMiddleware("MetadataBlobStorage", store)
	if err != nil {
		slog.Error(fmt.Sprint("Error during NewTracingStorageMiddleware: ", err))
		os.Exit(1)
	}

	return store
}
