package cache

import (
	"log"
	"os"
	"testing"

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
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		log.Fatalf("Could not create SqlMetadataStore: %s", err)
	}

	metadataBlobStorage, err := metadatablob.NewStorage(db, metadataStore, blobStore)
	if err != nil {
		log.Fatalf("Could not create MetadataBlobStorage: %s", err)
	}

	var cachePersistor persistor.CachePersistor
	/*
		cachePersistor, err = cache.NewInMemoryCachePersistor()
		if err != nil {
			log.Fatalf("Could not create InMemoryCacheStorage: %s", err)
		}
	*/

	tmpDir := os.TempDir()
	cachePersistor, err = filesystem.New(tmpDir)
	if err != nil {
		log.Fatalf("Could not create FilesystemCachePersistor: %s", err)
	}

	fixedSizeLimitEvictionChecker, err := fixedsizelimit.New(2000000000)
	if err != nil {
		log.Fatalf("Could not create FixedSizeLimitEvictionChecker: %s", err)
	}
	lfuCacheEvictionPolicy, err := lfu.New(fixedSizeLimitEvictionChecker)
	if err != nil {
		log.Fatalf("Could not create LFUCacheEvictionPolicy: %s", err)
	}

	cache, err := NewGenericCache(cachePersistor, lfuCacheEvictionPolicy)
	if err != nil {
		log.Fatalf("Could not create Cache: %s", err)
	}

	cacheStorage, err := New(cache, metadataBlobStorage)
	if err != nil {
		log.Fatalf("Could not create CacheStorage: %s", err)
	}

	content := []byte("CacheStorage")
	err = storage.Tester(cacheStorage, content)
	assert.Nil(t, err)
}
