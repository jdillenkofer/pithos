package outbox

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlobOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/bloboutboxentry/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	filesystemBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/filesystem"
	"github.com/stretchr/testify/assert"
)

func TestOutboxBlobStore(t *testing.T) {
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
	filesystemBlobStore, err := filesystemBlobStore.New(storagePath)
	if err != nil {
		log.Fatalf("Could not create FilesystemBlobStore: %s", err)
	}
	blobOutboxEntryRepository, err := sqliteBlobOutboxEntry.NewRepository(db)
	if err != nil {
		log.Fatalf("Could not create BlobOutboxEntryRepository: %s", err)
	}
	outboxBlobStore, err := New(db, filesystemBlobStore, blobOutboxEntryRepository)
	if err != nil {
		log.Fatalf("Could not create OutboxBlobStore: %s", err)
	}
	content := []byte("OutboxBlobStore")
	err = blobstore.Tester(outboxBlobStore, db, content)
	assert.Nil(t, err)
}
