package config

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func createStorageFromJson(b []byte) (storage.Storage, error) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		return nil, err
	}
	dbContainer := config.NewDbContainer()
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*config.DbContainer)(nil)), dbContainer)
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

func setupPostgresContainer(ctx context.Context) (*postgres.PostgresContainer, error) {
	username := "postgres"
	password := "postgres"
	dbname := "postgres"
	postgresContainer, err := postgres.Run(ctx, "postgres:17.5-alpine3.22",
		postgres.WithUsername(username),
		postgres.WithPassword(password),
		postgres.WithDatabase(dbname),
		postgres.BasicWaitStrategies())
	if err != nil {
		return nil, err
	}
	return postgresContainer, nil
}

func TestCanCreateMetadataBlobStorageWithPostgresFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := t.Context()
	pgContainer, err := setupPostgresContainer(ctx)
	assert.Nil(t, err)
	dbUrl, err := pgContainer.ConnectionString(ctx)
	assert.Nil(t, err)
	defer pgContainer.Terminate(ctx)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
	  "type": "MetadataBlobStorage",
	  "db": {
	    "type": "RegisterDatabaseReference",
		"refName": "db",
		"db": {
	      "type": "PostgresDatabase",
	      "dbUrl": "%v"
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
		"root": "%v"
	  }
	}`, dbUrl, storagePath)

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateMetadataBlobStorageFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
	  "type": "MetadataBlobStorage",
	  "db": {
	    "type": "RegisterDatabaseReference",
		"refName": "db",
		"db": {
	      "type": "SqliteDatabase",
	      "dbPath": "%v"
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
		"root": "%v"
	  }
	}`, dbPath, storagePath)

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateCacheStorageFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
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
		    "dbPath": "%v"
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
		  "root": "%v"
	    }
	  }
	}`, dbPath, storagePath)

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateConditionalStorageMiddlewareFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
	  "type": "ConditionalStorageMiddleware",
	  "bucketToStorageMap": {
		"test": {
			"type": "MetadataBlobStorage",
			"db": {
				"type": "RegisterDatabaseReference",
				"refName": "db",
				"db": {
					"type": "SqliteDatabase",
					"dbPath": "%v"
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
				"root": "%v"
			}
		}
	  },
	  "defaultStorage": {
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
				"type": "SqlBlobStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				}
			}
		}
	}`, dbPath, storagePath)

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreatePrometheusStorageMiddlewareFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
	  "type": "PrometheusStorageMiddleware",
	  "innerStorage": {
	    "type": "MetadataBlobStorage",
		"db": {
	      "type": "RegisterDatabaseReference",
		  "refName": "db",
		  "db": {
	        "type": "SqliteDatabase",
		    "dbPath": "%v"
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
		  "root": "%v"
	    }
	  }
	}`, dbPath, storagePath)

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateTracingStorageMiddlewareFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
	  "type": "TracingStorageMiddleware",
	  "regionName": "metadataBlobStorage",
	  "innerStorage": {
	    "type": "MetadataBlobStorage",
		"db": {
	      "type": "RegisterDatabaseReference",
		  "refName": "db",
		  "db": {
	        "type": "SqliteDatabase",
		    "dbPath": "%v"
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
		  "root": "%v"
	    }
	  }
	}`, dbPath, storagePath)

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateOutboxStorageFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
	  "type": "OutboxStorage",
	  "db": {
	    "type": "RegisterDatabaseReference",
	    "refName": "db",
		"db": {
	      "type": "SqliteDatabase",
		  "dbPath": "%v"
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
		  "root": "%v"
	    }
	  }
	}`, dbPath, storagePath)

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateReplicationStorageFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
	  "type": "ReplicationStorage",
	  "primaryStorage": {
	    "type": "MetadataBlobStorage",
		"db": {
	      "type": "RegisterDatabaseReference",
		  "refName": "db",
		  "db": {
	        "type": "SqliteDatabase",
		    "dbPath": "%v"
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
		  "root": "%v"
	    }
	  },
	  "secondaryStorages": []
	}`, dbPath, storagePath)

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateReplicationStorageWithSecondaryStoragesFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
	  "type": "ReplicationStorage",
	  "primaryStorage": {
	    "type": "MetadataBlobStorage",
		"db": {
	      "type": "RegisterDatabaseReference",
		  "refName": "db",
		  "db": {
	        "type": "SqliteDatabase",
		    "dbPath": "%v"
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
		  "root": "%v"
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
		    "root": "%v"
	      }
		} 
	  ]
	}`, dbPath, storagePath, storagePath)

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateS3ClientStorageFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

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
