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
	err = si.RegisterReferences(diContainer)
	if err != nil {
		return nil, err
	}
	return si.Instantiate(diContainer)
}

func TestCanCreateMetadataBlobStorageFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "MetadataBlobStorage",
	  "db": {
	    "type": "RegisterDatabaseReference",
		"refName": "db",
		"db": {
	      "type": "SqliteDatabase",
	      "dbPath": "/tmp/pithos/pithos.db"
	    }
      },
	  "metadataStore": {
		"type": "SqlMetadataStore",
		"db": {
	      "type": "DatabaseReference",
		  "refName": "db"
	    }
	  },
	  "blobStore": {
	    "type": "FilesystemBlobStore",
		"root": "/tmp/pithos/"
	  }
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateCacheStorageFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
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
		"db": {
	      "type": "RegisterDatabaseReference",
		  "refName": "db",
		  "db": {
	        "type": "SqliteDatabase",
		    "dbPath": "/tmp/pithos/pithos.db"
	      }
        },
	    "metadataStore": {
		  "type": "SqlMetadataStore",
		  "db": {
	        "type": "DatabaseReference",
		    "refName": "db"
	      }
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/tmp/pithos/"
	    }
	  }
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreatePrometheusStorageMiddlewareFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "PrometheusStorageMiddleware",
	  "innerStorage": {
	    "type": "MetadataBlobStorage",
		"db": {
	      "type": "RegisterDatabaseReference",
		  "refName": "db",
		  "db": {
	        "type": "SqliteDatabase",
		    "dbPath": "/tmp/pithos/pithos.db"
	      }
        },
	    "metadataStore": {
		  "type": "SqlMetadataStore",
		  "db": {
	        "type": "DatabaseReference",
		    "refName": "db"
	      }
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/tmp/pithos/"
	    }
	  }
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateTracingStorageMiddlewareFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "TracingStorageMiddleware",
	  "regionName": "metadataBlobStorage",
	  "innerStorage": {
	    "type": "MetadataBlobStorage",
		"db": {
	      "type": "RegisterDatabaseReference",
		  "refName": "db",
		  "db": {
	        "type": "SqliteDatabase",
		    "dbPath": "/tmp/pithos/pithos.db"
	      }
        },
	    "metadataStore": {
		  "type": "SqlMetadataStore",
		  "db": {
	        "type": "DatabaseReference",
		    "refName": "db"
	      }
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/tmp/pithos/"
	    }
	  }
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateOutboxStorageFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "OutboxStorage",
	  "db": {
	    "type": "RegisterDatabaseReference",
	    "refName": "db",
		"db": {
	      "type": "SqliteDatabase",
		  "dbPath": "/tmp/pithos/pithos.db"
	    }
      },
	  "innerStorage": {
	    "type": "MetadataBlobStorage",
		"db": {
		  "type": "DatabaseReference",
		  "refName": "db"
	    },
	    "metadataStore": {
		  "type": "SqlMetadataStore",
		  "db": {
	        "type": "DatabaseReference",
		    "refName": "db"
	      }
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/tmp/pithos/"
	    }
	  }
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateReplicationStorageFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "ReplicationStorage",
	  "primaryStorage": {
	    "type": "MetadataBlobStorage",
		"db": {
	      "type": "RegisterDatabaseReference",
		  "refName": "db",
		  "db": {
	        "type": "SqliteDatabase",
		    "dbPath": "/tmp/pithos/pithos.db"
	      }
        },
	    "metadataStore": {
		  "type": "SqlMetadataStore",
		  "db": {
	        "type": "DatabaseReference",
		    "refName": "db"
	      }
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/tmp/pithos/"
	    }
	  },
	  "secondaryStorages": []
	}`
	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateReplicationStorageWithSecondaryStoragesFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "ReplicationStorage",
	  "primaryStorage": {
	    "type": "MetadataBlobStorage",
		"db": {
	      "type": "RegisterDatabaseReference",
		  "refName": "db",
		  "db": {
	        "type": "SqliteDatabase",
		    "dbPath": "/tmp/pithos/pithos.db"
	      }
        },
	    "metadataStore": {
		  "type": "SqlMetadataStore",
		  "db": {
	        "type": "DatabaseReference",
		    "refName": "db"
	      }
	    },
	    "blobStore": {
	      "type": "FilesystemBlobStore",
		  "root": "/tmp/pithos/"
	    }
	  },
	  "secondaryStorages": [
		{
		  "type": "MetadataBlobStorage",
		  "db": {
	        "type": "DatabaseReference",
		    "refName": "db"
	      },
		  "metadataStore": {
			"type": "SqlMetadataStore",
		    "db": {
	          "type": "DatabaseReference",
		      "refName": "db"
	        }
		  },
		  "blobStore": {
	        "type": "FilesystemBlobStore",
		    "root": "/tmp/pithos/"
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
