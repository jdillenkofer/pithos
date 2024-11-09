package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCanCreateMetadataBlobStorageFromJson(t *testing.T) {
	jsonData := `{
	  "type": "MetadataBlobStorage"
	}`
	storage, err := CreateStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateCacheStorageFromJson(t *testing.T) {
	jsonData := `{
	  "type": "CacheStorage",
	  "innerStorage": {
	    "type": "MetadataBlobStorage"
	  }
	}`
	storage, err := CreateStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreatePrometheusStorageMiddlewareFromJson(t *testing.T) {
	jsonData := `{
	  "type": "PrometheusStorageMiddleware",
	  "innerStorage": {
	    "type": "MetadataBlobStorage"
	  }
	}`
	storage, err := CreateStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateTracingStorageMiddlewareFromJson(t *testing.T) {
	jsonData := `{
	  "type": "TracingStorageMiddleware",
	  "regionName": "metadataBlobStorage",
	  "innerStorage": {
	    "type": "MetadataBlobStorage"
	  }
	}`
	storage, err := CreateStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateOutboxStorageFromJson(t *testing.T) {
	jsonData := `{
	  "type": "OutboxStorage",
	  "innerStorage": {
	    "type": "MetadataBlobStorage"
	  }
	}`
	storage, err := CreateStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateReplicationStorageFromJson(t *testing.T) {
	jsonData := `{
	  "type": "ReplicationStorage",
	  "primaryStorage": {
	    "type": "MetadataBlobStorage"
	  },
	  "secondaryStorages": []
	}`
	storage, err := CreateStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateReplicationStorageWithSecondaryStoragesFromJson(t *testing.T) {
	jsonData := `{
	  "type": "ReplicationStorage",
	  "primaryStorage": {
	    "type": "MetadataBlobStorage"
	  },
	  "secondaryStorages": [
		{
			"type": "MetadataBlobStorage"
		} 
	  ]
	}`
	storage, err := CreateStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateS3ClientStorageFromJson(t *testing.T) {
	jsonData := `{
	  "type": "S3ClientStorage"
	}`
	storage, err := CreateStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}
