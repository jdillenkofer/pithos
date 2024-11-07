package storage

import (
	"log"
	"os"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/cache"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
	"github.com/stretchr/testify/assert"
)

func TestCacheStorage(t *testing.T) {
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		log.Fatalf("Could not create temp directory: %s", err)
	}
	db, err := database.OpenDatabase(storagePath)
	if err != nil {
		log.Fatal("Couldn't open database")
	}
	defer func() {
		err = db.Close()
		if err != nil {
			log.Fatalf("Could not close database %s", err)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			log.Fatalf("Could not remove storagePath %s: %s", storagePath, err)
		}
	}()

	blobStore, err := blob.NewSqlBlobStore(db)
	if err != nil {
		log.Fatalf("Could not create SqlBlobStore: %s", err)
	}

	metadataStore, err := metadata.NewSqlMetadataStore(db)
	if err != nil {
		log.Fatalf("Could not create SqlMetadataStore: %s", err)
	}

	metadataBlobStorage, err := NewMetadataBlobStorage(db, metadataStore, blobStore)
	if err != nil {
		log.Fatalf("Could not create MetadataBlobStorage: %s", err)
	}

	var cachePersistor cache.CachePersistor
	/*
		cachePersistor, err = cache.NewInMemoryCachePersistor()
		if err != nil {
			log.Fatalf("Could not create InMemoryCacheStorage: %s", err)
		}
	*/

	tmpDir := os.TempDir()
	cachePersistor, err = cache.NewFilesystemCachePersistor(tmpDir)
	if err != nil {
		log.Fatalf("Could not create FilesystemCachePersistor: %s", err)
	}

	fixedSizeLimitEvictionChecker, err := cache.NewFixedSizeLimitEvictionChecker(2000000000)
	if err != nil {
		log.Fatalf("Could not create FixedSizeLimitEvictionChecker: %s", err)
	}
	lfuCacheEvictionPolicy, err := cache.NewLFUCacheEvictionPolicy(fixedSizeLimitEvictionChecker)
	if err != nil {
		log.Fatalf("Could not create LFUCacheEvictionPolicy: %s", err)
	}

	cache, err := cache.NewGenericCache(cachePersistor, lfuCacheEvictionPolicy)
	if err != nil {
		log.Fatalf("Could not create Cache: %s", err)
	}

	cacheStorage, err := NewCacheStorage(cache, metadataBlobStorage)
	if err != nil {
		log.Fatalf("Could not create CacheStorage: %s", err)
	}

	content := []byte("CacheStorage")
	err = StorageTester(cacheStorage, content)
	assert.Nil(t, err)
}
