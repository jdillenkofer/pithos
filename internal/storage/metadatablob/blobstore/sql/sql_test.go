package sql

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlobContent "github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/stretchr/testify/assert"
)

func TestSqlBlobStore(t *testing.T) {
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		log.Fatalf("Could not create temp directory: %s", err)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := database.OpenDatabase(dbPath)
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
	blobContentRepository, err := sqliteBlobContent.NewRepository()
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
