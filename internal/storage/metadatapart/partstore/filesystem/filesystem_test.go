package filesystem

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestFilesystemPartStoreCanConvertFilenameAndPartId(t *testing.T) {
	testutils.SkipIfIntegration(t)

	filesystemPartStore := filesystemPartStore{root: "."}
	partId, err := partstore.NewRandomPartId()
	if err != nil {
		t.Fatalf("Failed to generate PartId: %v", err)
	}
	filename := filesystemPartStore.getFilename(*partId)
	partId2, ok := filesystemPartStore.tryGetPartIdFromFilename(filename)
	assert.True(t, ok)
	assert.Equal(t, *partId, *partId2)
}

func TestFilesystemPartStore(t *testing.T) {
	testutils.SkipIfIntegration(t)

	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
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
	filesystemPartStore, err := New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemPartStore: %s", err))
		os.Exit(1)
	}
	content := []byte("FilesystemPartStore")
	err = partstore.Tester(filesystemPartStore, db, content)
	assert.Nil(t, err)
}
