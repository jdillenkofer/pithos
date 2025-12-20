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
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	sqlPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestConditionalStorage(t *testing.T) {
	testutils.SkipIfIntegration(t)
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
	dbPath2 := filepath.Join(storagePath, "pithos2.db")
	db2, err := sqlite.OpenDatabase(dbPath2)
	if err != nil {
		slog.Error("Couldn't open database 2")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = db2.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database 2 %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()

	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartContentRepository: %s", err))
		os.Exit(1)
	}
	partStore, err := sqlPartStore.New(db, partContentRepository)
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
		slog.Error(fmt.Sprintf("Could not create MetadataPartStorage: %s", err))
		os.Exit(1)
	}

	fsPartStore, err := filesystemPartStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemPartStore: %s", err))
		os.Exit(1)
	}

	bucketRepository2, err := repositoryFactory.NewBucketRepository(db2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BucketRepository 2: %s", err))
		os.Exit(1)
	}
	objectRepository2, err := repositoryFactory.NewObjectRepository(db2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ObjectRepository 2: %s", err))
		os.Exit(1)
	}
	partRepository2, err := repositoryFactory.NewPartRepository(db2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartRepository 2: %s", err))
		os.Exit(1)
	}
	metadataStore2, err := sqlMetadataStore.New(db2, bucketRepository2, objectRepository2, partRepository2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlMetadataStore 2: %s", err))
		os.Exit(1)
	}

	metadataPartStorage2, err := metadatapart.NewStorage(db2, metadataStore2, fsPartStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create MetadataPartStorage: %s", err))
		os.Exit(1)
	}

	bucketToStorageMap := map[string]storage.Storage{"bucket": metadataPartStorage}
	conditionalStorage, err := NewStorageMiddleware(bucketToStorageMap, metadataPartStorage2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ConditionalStorage: %s", err))
		os.Exit(1)
	}

	content := []byte("ConditionalStorage")
	err = storage.Tester(conditionalStorage, []storage.BucketName{storage.MustNewBucketName("bucket"), storage.MustNewBucketName("otherbucket")}, content)
	assert.Nil(t, err)
}
