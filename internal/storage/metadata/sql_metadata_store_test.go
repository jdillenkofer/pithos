package metadata

import (
	"log"
	"os"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlobRepository "github.com/jdillenkofer/pithos/internal/storage/database/repository/blob/sqlite"
	sqliteBucketRepository "github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket/sqlite"
	sqliteObjectRepository "github.com/jdillenkofer/pithos/internal/storage/database/repository/object/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestSqlMetadataStore(t *testing.T) {
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

	bucketRepository, err := sqliteBucketRepository.New(db)
	if err != nil {
		log.Fatalf("Could not create BucketRepository: %s", err)
	}
	objectRepository, err := sqliteObjectRepository.New(db)
	if err != nil {
		log.Fatalf("Could not create ObjectRepository: %s", err)
	}
	blobRepository, err := sqliteBlobRepository.New(db)
	if err != nil {
		log.Fatalf("Could not create BlobRepository: %s", err)
	}
	sqlMetadataStore, err := NewSqlMetadataStore(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		log.Fatalf("Could not create SqlMetadataStore: %s", err)
	}
	err = MetadataStoreTester(sqlMetadataStore, db)
	assert.Nil(t, err)
}
