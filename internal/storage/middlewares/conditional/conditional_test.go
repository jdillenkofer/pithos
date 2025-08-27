package conditional

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob"
	filesystemBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/filesystem"
	sqlBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/sql"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/sql"
	"github.com/stretchr/testify/assert"
)

func TestConditionalStorage(t *testing.T) {
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
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

	blobContentRepository, err := repositoryFactory.NewBlobContentRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BlobContentRepository: %s", err))
		os.Exit(1)
	}
	blobStore, err := sqlBlobStore.New(db, blobContentRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlBlobStore: %s", err))
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
	blobRepository, err := repositoryFactory.NewBlobRepository(db)
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
		slog.Error(fmt.Sprintf("Could not create MetadataBlobStorage: %s", err))
		os.Exit(1)
	}

	fsBlobStore, err := filesystemBlobStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemBlobStore: %s", err))
		os.Exit(1)
	}

	metadataBlobStorage2, err := metadatablob.NewStorage(db, metadataStore, fsBlobStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create MetadataBlobStorage: %s", err))
		os.Exit(1)
	}

	bucketToStorageMap := map[string]storage.Storage{"bucket": metadataBlobStorage}
	conditionalStorage, err := NewStorageMiddleware(bucketToStorageMap, metadataBlobStorage2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ConditionalStorage: %s", err))
		os.Exit(1)
	}

	content := []byte("ConditionalStorage")
	err = storage.Tester(conditionalStorage, []string{"bucket", "otherbucket"}, content)
	assert.Nil(t, err)
}
