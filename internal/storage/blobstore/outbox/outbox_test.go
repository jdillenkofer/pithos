package outbox

import (
	"log"
	"os"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/blobstore"
	filesystemBlobStore "github.com/jdillenkofer/pithos/internal/storage/blobstore/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteBlobOutboxEntryRepository "github.com/jdillenkofer/pithos/internal/storage/repository/bloboutboxentry/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestOutboxBlobStore(t *testing.T) {
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
	filesystemBlobStore, err := filesystemBlobStore.New(storagePath)
	if err != nil {
		log.Fatalf("Could not create FilesystemBlobStore: %s", err)
	}
	blobOutboxEntryRepository, err := sqliteBlobOutboxEntryRepository.New(db)
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
