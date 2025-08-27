package sql

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/stretchr/testify/assert"
)

func TestSqlBlobStore(t *testing.T) {
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
	blobContentRepository, err := repositoryFactory.NewBlobContentRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BlobContentRepository: %s", err))
		os.Exit(1)
	}
	sqlBlobStore, err := New(db, blobContentRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlBlobStore: %s", err))
		os.Exit(1)
	}
	content := []byte("SqlBlobStore")
	err = blobstore.Tester(sqlBlobStore, db, content)
	assert.Nil(t, err)
}
