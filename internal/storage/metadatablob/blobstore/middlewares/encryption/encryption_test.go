package encryption

import (
	"fmt"
	"log/slog"
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
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := database.OpenDatabase(dbPath)
	if err != nil {
		slog.Error("Couldn't open database")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()
	filesystemBlobStore, err := filesystemBlobStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemBlobStore: %s", err))
		os.Exit(1)
	}
	encryptionBlobStoreMiddleware, err := New("password", filesystemBlobStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create EncryptionBlobStoreMiddleware: %s", err))
		os.Exit(1)
	}
	content := []byte("EncryptionBlobStoreMiddleware")
	err = blobstore.Tester(encryptionBlobStoreMiddleware, db, content)
	assert.Nil(t, err)
}
