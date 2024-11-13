package config

import (
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func createStorageFromJson(b []byte) (storage.Storage, error) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		return nil, err
	}
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*prometheus.Registerer)(nil)), prometheus.NewRegistry())
	if err != nil {
		return nil, err
	}
	si, err := CreateStorageInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	return si.Instantiate(diContainer)
}

func TestCanCreateMetadataBlobStorageFromJson(t *testing.T) {
	jsonData := `{
	  "type": "MetadataBlobStorage",
	  "metadataStore": {
		"type": "SqlMetadataStore"
	  },
	  "blobStore": {
	    "type": "FilesystemBlobStore",
		"root": "/"
	  }
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateCacheStorageFromJson(t *testing.T) {
	jsonData := `{
	  "type": "CacheStorage",
	  "cache": {
	    "type": "GenericCache",
		"cachePesistor": {
		  "type": "InMemoryPersistor"
		},
		"cacheEvictionPolicy": {
	      "type": "EvictNothingEvictionPolicy"
		}
	  },
	  "innerStorage": {
	    "type": "MetadataBlobStorage",
	    "metadataStore": {
		  "type": "SqlMetadataStore"
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/"
	    }
	  }
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreatePrometheusStorageMiddlewareFromJson(t *testing.T) {
	jsonData := `{
	  "type": "PrometheusStorageMiddleware",
	  "innerStorage": {
	    "type": "MetadataBlobStorage",
	    "metadataStore": {
		  "type": "SqlMetadataStore"
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/"
	    }
	  }
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateTracingStorageMiddlewareFromJson(t *testing.T) {
	jsonData := `{
	  "type": "TracingStorageMiddleware",
	  "regionName": "metadataBlobStorage",
	  "innerStorage": {
	    "type": "MetadataBlobStorage",
	    "metadataStore": {
		  "type": "SqlMetadataStore"
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/"
	    }
	  }
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateOutboxStorageFromJson(t *testing.T) {
	jsonData := `{
	  "type": "OutboxStorage",
	  "innerStorage": {
	    "type": "MetadataBlobStorage",
	    "metadataStore": {
		  "type": "SqlMetadataStore"
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/"
	    }
	  }
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateReplicationStorageFromJson(t *testing.T) {
	jsonData := `{
	  "type": "ReplicationStorage",
	  "primaryStorage": {
	    "type": "MetadataBlobStorage",
	    "metadataStore": {
		  "type": "SqlMetadataStore"
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/"
	    }
	  },
	  "secondaryStorages": []
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateReplicationStorageWithSecondaryStoragesFromJson(t *testing.T) {
	jsonData := `{
	  "type": "ReplicationStorage",
	  "primaryStorage": {
	    "type": "MetadataBlobStorage",
	    "metadataStore": {
		  "type": "SqlMetadataStore"
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/"
	    }
	  },
	  "secondaryStorages": [
		{
		  "type": "MetadataBlobStorage",
		  "metadataStore": {
			"type": "SqlMetadataStore"
		  },
		  "blobStore": {
	        "type": "FilesystemBlobStore",
		    "root": "/"
	      }
		} 
	  ]
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateS3ClientStorageFromJson(t *testing.T) {
	jsonData := `{
	  "type": "S3ClientStorage",
	  "baseEndpoint": "http://localhost:9090/",
	  "region": "eu-central-1",
	  "accessKeyId": "abc",
	  "secretAccessKey": "def",
	  "usePathStyle": false
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}
