package cache

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker/fixedsizelimit"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/lfu"
	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor"
	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlob "github.com/jdillenkofer/pithos/internal/storage/database/repository/blob/sqlite"
	sqliteBlobContent "github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent/sqlite"
	sqliteBucket "github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket/sqlite"
	sqliteObject "github.com/jdillenkofer/pithos/internal/storage/database/repository/object/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob"
	sqlBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/sql"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/sql"
	"github.com/stretchr/testify/assert"
)

func TestCacheStorage(t *testing.T) {
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

	blobContentRepository, err := sqliteBlobContent.NewRepository()
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BlobContentRepository: %s", err))
		os.Exit(1)
	}
	blobStore, err := sqlBlobStore.New(db, blobContentRepository)
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
		slog.Error(fmt.Sprintf("Could not create MetadataBlobStorage: %s", err))
		os.Exit(1)
	}

	var cachePersistor persistor.CachePersistor
	/*
				cachePersistor, err = cache.NewInMemoryCachePersistor()
				if err != nil {
					slog.Error(fmt.Sprintf("Could not create InMemoryCacheStorage: %s", err))
		os.Exit(1)
				}
	*/

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)
	cachePersistor, err = filesystem.New(*tempDir)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemCachePersistor: %s", err))
		os.Exit(1)
	}

	fixedSizeLimitEvictionChecker, err := fixedsizelimit.New(2000000000)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FixedSizeLimitEvictionChecker: %s", err))
		os.Exit(1)
	}
	lfuCacheEvictionPolicy, err := lfu.New(fixedSizeLimitEvictionChecker)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create LFUCacheEvictionPolicy: %s", err))
		os.Exit(1)
	}

	cache, err := NewGenericCache(cachePersistor, lfuCacheEvictionPolicy)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create Cache: %s", err))
		os.Exit(1)
	}

	cacheStorage, err := New(cache, metadataBlobStorage)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create CacheStorage: %s", err))
		os.Exit(1)
	}

	content := []byte("CacheStorage")
	err = storage.Tester(cacheStorage, []string{"bucket"}, content)
	assert.Nil(t, err)
}
