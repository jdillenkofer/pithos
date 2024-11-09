package sql

import (
	"log"
	"os"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlob "github.com/jdillenkofer/pithos/internal/storage/database/repository/blob/sqlite"
	sqliteBucket "github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket/sqlite"
	sqliteObject "github.com/jdillenkofer/pithos/internal/storage/database/repository/object/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatastore"
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
	sqlMetadataStore, err := New(db, bucketRepository, objectRepository, blobRepository)
	if err != nil {
		log.Fatalf("Could not create SqlMetadataStore: %s", err)
	}
	err = metadatastore.MetadataStoreTester(sqlMetadataStore, db)
	assert.Nil(t, err)
}
