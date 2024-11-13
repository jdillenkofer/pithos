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
	return mi.Instantiate(diContainer)
}

func TestCanCreateFilesystemBlobStoreFromJson(t *testing.T) {
	jsonData := `{
	  "type": "FilesystemBlobStore",
	  "root": "/tmp/pithos/"
	}`
	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateEncryptionBlobStoreMiddlewareFromJson(t *testing.T) {
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
	jsonData := `{
	  "type": "OutboxBlobStore",
	  "db": {
	    "type": "SqliteDatabase",
		"storagePath": "/tmp/pithos/"
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
	jsonData := `{
	  "type": "SqlBlobStore",
	  "db": {
	    "type": "SqliteDatabase",
		"storagePath": "/tmp/pithos/"
	  }
	}`
	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}
