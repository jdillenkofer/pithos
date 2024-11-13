package config

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/stretchr/testify/assert"
)

func createBlobStoreFromJson(b []byte) (blobstore.BlobStore, error) {
	diContainer, err := dependencyinjection.NewContainer()
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
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "FilesystemBlobStore",
	  "root": "/tmp/pithos/"
	}`
	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateEncryptionBlobStoreMiddlewareFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "EncryptionBlobStoreMiddleware",
	  "innerBlobStore": {
	    "type": "FilesystemBlobStore",
	    "root": "/tmp/pithos/"
	  }
	}`
	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateTracingBlobStoreMiddlewareFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "TracingBlobStoreMiddleware",
	  "regionName": "FilesystemBlobStore",
	  "innerBlobStore": {
	    "type": "FilesystemBlobStore",
	    "root": "/tmp/pithos/"
	  }
	}`
	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateOutboxBlobStoreFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "OutboxBlobStore",
	  "db": {
	    "type": "SqliteDatabase",
		"dbPath": "/tmp/pithos/pithos.db"
	  },
	  "innerBlobStore": {
	    "type": "FilesystemBlobStore",
	    "root": "/tmp/pithos/"
	  }
	}`
	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateSqlBlobStoreFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "SqlBlobStore",
	  "db": {
	    "type": "SqliteDatabase",
		"dbPath": "/tmp/pithos/pithos.db"
	  }
	}`
	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}
