package sql

import (
	"log"
	"os"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlobContentRepository "github.com/jdillenkofer/pithos/internal/storage/repository/blobcontent/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestSqlBlobStore(t *testing.T) {
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
	blobContentRepository, err := sqliteBlobContentRepository.New(db)
	if err != nil {
		log.Fatalf("Could not create BlobContentRepository: %s", err)
	}
	sqlBlobStore, err := New(db, blobContentRepository)
	if err != nil {
		log.Fatalf("Could not create SqlBlobStore: %s", err)
	}
	content := []byte("SqlBlobStore")
	err = blobstore.Tester(sqlBlobStore, db, content)
	assert.Nil(t, err)
}
