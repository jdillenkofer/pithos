package storage

import (
	"log"
	"os"
	"testing"

	sqlBlobStore "github.com/jdillenkofer/pithos/internal/storage/blobstore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/cache"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlob "github.com/jdillenkofer/pithos/internal/storage/database/repository/blob/sqlite"
	sqliteBlobContent "github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent/sqlite"
	sqliteBucket "github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket/sqlite"
	sqliteObject "github.com/jdillenkofer/pithos/internal/storage/database/repository/object/sqlite"
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

	blobContentRepository, err := sqliteBlobContent.NewRepository(db)
	if err != nil {
		log.Fatalf("Could not create BlobContentRepository: %s", err)
	}
	blobStore, err := sqlBlobStore.New(db, blobContentRepository)
	if err != nil {
		log.Fatalf("Could not create SqlBlobStore: %s", err)
	}

	bucketRepository, err := sqliteBucket.NewRepository(db)
	if err != nil {
		log.Fatalf("Could not create BucketRepository: %s", err)
	}
	objectRepository, err := sqliteObject.NewRepository(db)
	if err != nil {
		log.Fatalf("Could not create ObjectRepository: %s", err)
	}
	blobRepository, err := sqliteBlob.NewRepository(db)
	if err != nil {
		log.Fatalf("Could not create BlobRepository: %s", err)
	}
	metadataStore, err := metadata.NewSqlMetadataStore(db, bucketRepository, objectRepository, blobRepository)
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
