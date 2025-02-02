package storagefactory

import (
	"log"
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
	metadataStore, err = sqlMetadataStore.New(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		log.Fatal("Error during NewSqlMetadataStore: ", err)
	}

	metadataStore, err = tracingMetadataStoreMiddleware.New("SqlMetadataStore", metadataStore)
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
		blobContentRepository, err := sqliteBlobContent.NewRepository()
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
		blobOutboxEntryRepository, err := sqliteBlobOutboxEntry.NewRepository()
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
	var store storage.Storage
	store, err = metadatablob.NewStorage(db, metadataStore, blobStore)
	if err != nil {
		log.Fatal("Error during NewMetadataBlobStorage: ", err)
	}

	store, err = tracingStorageMiddleware.NewStorageMiddleware("MetadataBlobStorage", store)
	if err != nil {
		log.Fatal("Error during NewTracingStorageMiddleware: ", err)
	}

	return store
}
