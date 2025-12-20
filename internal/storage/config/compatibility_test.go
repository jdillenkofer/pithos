package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackwardCompatibility_MetadataBlobStorage(t *testing.T) {
	// JSON using the old "MetadataBlobStorage" type and "blobStore" field
	configJSON := []byte(`{
		"type": "MetadataBlobStorage",
		"db": {
			"type": "RegisterDatabaseReference",
			"refName": "mainDb",
			"db": {
				"type": "SqliteDatabase",
				"dbPath": ":memory:"
			}
		},
		"metadataStore": {
			"type": "SqlMetadataStore",
			"db": {
				"type": "DatabaseReference",
				"refName": "mainDb"
			}
		},
		"blobStore": {
			"type": "FilesystemBlobStore",
			"root": "/tmp/pithos-test"
		}
	}`)

	instantiator, err := CreateStorageInstantiatorFromJson(configJSON)
	assert.NoError(t, err)
	assert.NotNil(t, instantiator)

	// Verify it was parsed into the new structure alias
	blobConfig, ok := instantiator.(*MetadataBlobStorageConfiguration)
	assert.True(t, ok, "Expected MetadataBlobStorageConfiguration")

	// Verify the inner part store (blobStore) was parsed correctly
	assert.NotNil(t, blobConfig.PartStoreInstantiator, "PartStoreInstantiator should not be nil")
}

func TestBackwardCompatibility_TinkEncryptionInnerBlobStore(t *testing.T) {
	// JSON using "innerBlobStore" instead of "innerPartStore" inside Tink middleware
	// We verify if we can construct a full storage config that uses this.

	fullConfigJSON := []byte(`{
		"type": "MetadataPartStorage",
		"db": {
			"type": "RegisterDatabaseReference",
			"refName": "mainDb",
			"db": {
				"type": "SqliteDatabase",
				"dbPath": ":memory:"
			}
		},
		"metadataStore": {
			"type": "SqlMetadataStore",
			"db": {
				"type": "DatabaseReference",
				"refName": "mainDb"
			}
		},
		"partStore": {
			"type": "TinkEncryptionBlobStoreMiddleware",
			"kmsType": "local",
			"password": "test-password",
			"innerBlobStore": {
				"type": "FilesystemBlobStore",
				"root": "/tmp/pithos-test-enc"
			}
		}
	}`)

	instantiator, err := CreateStorageInstantiatorFromJson(fullConfigJSON)
	assert.NoError(t, err)
	
	metaConfig, ok := instantiator.(*MetadataPartStorageConfiguration)
	assert.True(t, ok)
	assert.NotNil(t, metaConfig.PartStoreInstantiator)
}

func TestBackwardCompatibility_OutboxBlobStore(t *testing.T) {
	configJSON := []byte(`{
		"type": "MetadataPartStorage",
		"db": {
			"type": "RegisterDatabaseReference",
			"refName": "mainDb",
			"db": {
				"type": "SqliteDatabase",
				"dbPath": ":memory:"
			}
		},
		"metadataStore": {
			"type": "SqlMetadataStore",
			"db": {
				"type": "DatabaseReference",
				"refName": "mainDb"
			}
		},
		"partStore": {
			"type": "OutboxBlobStore",
			"db": {
				"type": "DatabaseReference",
				"refName": "mainDb"
			},
			"innerBlobStore": {
				"type": "SqlBlobStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "mainDb"
				}
			}
		}
	}`)

	instantiator, err := CreateStorageInstantiatorFromJson(configJSON)
	assert.NoError(t, err)
	assert.NotNil(t, instantiator)
}

func TestBackwardCompatibility_MetadataPartStorageWithBlobStoreField(t *testing.T) {
	configJSON := []byte(`{
		"type": "MetadataPartStorage",
		"db": {
			"type": "RegisterDatabaseReference",
			"refName": "mainDb",
			"db": {
				"type": "SqliteDatabase",
				"dbPath": ":memory:"
			}
		},
		"metadataStore": {
			"type": "SqlMetadataStore",
			"db": {
				"type": "DatabaseReference",
				"refName": "mainDb"
			}
		},
		"blobStore": {
			"type": "FilesystemPartStore",
			"root": "/tmp/pithos-test"
		}
	}`)

	instantiator, err := CreateStorageInstantiatorFromJson(configJSON)
	assert.NoError(t, err)
	assert.NotNil(t, instantiator)
}

func TestBackwardCompatibility_SftpBlobStore(t *testing.T) {
	configJSON := []byte(`{
		"type": "MetadataPartStorage",
		"db": {
			"type": "RegisterDatabaseReference",
			"refName": "mainDb",
			"db": {
				"type": "SqliteDatabase",
				"dbPath": ":memory:"
			}
		},
		"metadataStore": {
			"type": "SqlMetadataStore",
			"db": {
				"type": "DatabaseReference",
				"refName": "mainDb"
			}
		},
		"partStore": {
			"type": "SftpBlobStore",
			"addr": "localhost:22",
			"sshClientConfig": {
				"user": "test",
				"authMethods": [
					{
						"type": "PasswordAuthMethod",
						"password": "test-password"
					}
				],
				"hostKeyCallback": {
					"type": "InsecureIgnoreHostKeyCallback"
				}
			},
			"root": "/tmp/pithos-sftp"
		}
	}`)

	instantiator, err := CreateStorageInstantiatorFromJson(configJSON)
	assert.NoError(t, err)
	assert.NotNil(t, instantiator)
}

func TestBackwardCompatibility_CachePesistorTypo(t *testing.T) {
	configJSON := []byte(`{
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
			"type": "MetadataPartStorage",
			"db": {
				"type": "SqliteDatabase",
				"dbPath": ":memory:"
			},
			"metadataStore": {
				"type": "SqlMetadataStore",
				"db": {
					"type": "SqliteDatabase",
					"dbPath": ":memory:"
				}
			},
			"partStore": {
				"type": "SqlPartStore",
				"db": {
					"type": "SqliteDatabase",
					"dbPath": ":memory:"
				}
			}
		}
	}`)

	instantiator, err := CreateStorageInstantiatorFromJson(configJSON)
	assert.NoError(t, err)
	assert.NotNil(t, instantiator)
}