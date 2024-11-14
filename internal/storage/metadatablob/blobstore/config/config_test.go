package config

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/stretchr/testify/assert"
)

func createBlobStoreFromJson(b []byte) (blobstore.BlobStore, error) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		return nil, err
	}
	dbContainer := config.NewDbContainer()
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*config.DbContainer)(nil)), dbContainer)
	if err != nil {
		return nil, err
	}
	mi, err := CreateBlobStoreInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	err = mi.RegisterReferences(diContainer)
	if err != nil {
		return nil, err
	}
	return mi.Instantiate(diContainer)
}

func TestCanCreateFilesystemBlobStoreFromJson(t *testing.T) {
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
	  "type": "FilesystemBlobStore",
	  "root": "%v"
	}`, storagePath)

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateEncryptionBlobStoreMiddlewareFromJson(t *testing.T) {
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
	  "type": "EncryptionBlobStoreMiddleware",
	  "innerBlobStore": {
	    "type": "FilesystemBlobStore",
	    "root": "%v"
	  }
	}`, storagePath)

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateTracingBlobStoreMiddlewareFromJson(t *testing.T) {
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
	  "type": "TracingBlobStoreMiddleware",
	  "regionName": "FilesystemBlobStore",
	  "innerBlobStore": {
	    "type": "FilesystemBlobStore",
	    "root": "%v"
	  }
	}`, storagePath)

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateOutboxBlobStoreFromJson(t *testing.T) {
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
	  "type": "OutboxBlobStore",
	  "db": {
	    "type": "SqliteDatabase",
		"dbPath": "%v"
	  },
	  "innerBlobStore": {
	    "type": "FilesystemBlobStore",
	    "root": "%v"
	  }
	}`, dbPath, storagePath)

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateSqlBlobStoreFromJson(t *testing.T) {
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
	  "type": "SqlBlobStore",
	  "db": {
	    "type": "SqliteDatabase",
		"dbPath": "%v"
	  }
	}`, dbPath)

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}
