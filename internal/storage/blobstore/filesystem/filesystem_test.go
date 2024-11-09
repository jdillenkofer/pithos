package filesystem

import (
	"log"
	"os"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

func TestFilesystemBlobStoreCanConvertFilenameAndBlobId(t *testing.T) {
	filesystemBlobStore := filesystemBlobStore{"."}
	blobId := ulid.Make()
	filename := filesystemBlobStore.getFilename(blobId)
	blobId2, ok := filesystemBlobStore.tryGetBlobIdFromFilename(filename)
	assert.True(t, ok)
	assert.Equal(t, blobId, *blobId2)
}

func TestFilesystemBlobStore(t *testing.T) {
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
	filesystemBlobStore, err := New(storagePath)
	if err != nil {
		log.Fatalf("Could not create FilesystemBlobStore: %s", err)
	}
	content := []byte("FilesystemBlobStore")
	err = blobstore.Tester(filesystemBlobStore, db, content)
	assert.Nil(t, err)
}
