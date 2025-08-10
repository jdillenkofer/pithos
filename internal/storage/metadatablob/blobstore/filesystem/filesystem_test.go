package filesystem

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
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
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := database.OpenDatabase(database.DB_TYPE_SQLITE, dbPath)
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
	filesystemBlobStore, err := New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemBlobStore: %s", err))
		os.Exit(1)
	}
	content := []byte("FilesystemBlobStore")
	err = blobstore.Tester(filesystemBlobStore, db, content)
	assert.Nil(t, err)
}
