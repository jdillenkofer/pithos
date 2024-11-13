package encryption

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	filesystemBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/filesystem"
	"github.com/stretchr/testify/assert"
)

func TestEncryptionBlobStore(t *testing.T) {
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
	encryptionBlobStoreMiddleware, err := New("password", filesystemBlobStore)
	if err != nil {
		log.Fatalf("Could not create EncryptionBlobStoreMiddleware: %s", err)
	}
	content := []byte("EncryptionBlobStoreMiddleware")
	err = blobstore.Tester(encryptionBlobStoreMiddleware, db, content)
	assert.Nil(t, err)
}
