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
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	sqlPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestCacheStorage(t *testing.T) {
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

	cacheStorage, err := New(cache, metadataPartStorage)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create CacheStorage: %s", err))
		os.Exit(1)
	}

	content := []byte("CacheStorage")
	err = storage.Tester(cacheStorage, []storage.BucketName{storage.MustNewBucketName("bucket")}, content)
	assert.Nil(t, err)
}
