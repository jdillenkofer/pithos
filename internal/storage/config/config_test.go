package config

import (
	"context"
	"crypto/ed25519"
	"crypto/mldsa"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/notification"
	_ "github.com/jdillenkofer/pithos/internal/testing"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
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
	postgresContainer, err := testutils.RunContainer(func() (*postgres.PostgresContainer, error) {
		return postgres.Run(ctx, testutils.PostgresImage,
			postgres.WithUsername(username),
			postgres.WithPassword(password),
			postgres.WithDatabase(dbname),
			postgres.BasicWaitStrategies())
	})
	if err != nil {
		return nil, err
	}
	return postgresContainer, nil
}

func TestCanCreateMetadataPartStorageWithPostgresFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)

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
			"type": "MetadataPartStorage",
			"db": {
				"type": "RegisterDatabaseReference",
				"refName": "db",
				"db": {
					"type": "PostgresDatabase",
					"dbUrl": %s
				}
			},
			"metadataStore": {
				"type": "SqlMetadataStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				}
			},
			"partStore": {
				"type": "FilesystemPartStore",
				"root": %s
			}
		}`, strconv.Quote(dbUrl), strconv.Quote(storagePath))

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateMetadataPartStorageFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
			"type": "MetadataPartStorage",
			"db": {
				"type": "RegisterDatabaseReference",
				"refName": "db",
				"db": {
					"type": "SqliteDatabase",
					"dbPath": %s
				}
			},
			"metadataStore": {
				"type": "SqlMetadataStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				}
			},
			"partStore": {
				"type": "FilesystemPartStore",
				"root": %s
			}
		}`, strconv.Quote(dbPath), strconv.Quote(storagePath))

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func metadataPartStorageJsonWithNotifications(t *testing.T, notificationsFragment string) []byte {
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	return fmt.Appendf(nil, `{
			"type": "MetadataPartStorage",
			"db": {
				"type": "RegisterDatabaseReference",
				"refName": "db",
				"db": {
					"type": "SqliteDatabase",
					"dbPath": %s
				}
			},
			"metadataStore": {
				"type": "SqlMetadataStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				}
			},
			"partStore": {
				"type": "FilesystemPartStore",
				"root": %s
			}%s
		}`, strconv.Quote(dbPath), strconv.Quote(storagePath), notificationsFragment)
}

func TestMetadataPartStorageEnablesNotificationsByDefault(t *testing.T) {
	testutils.SkipIfIntegration(t)

	store, err := createStorageFromJson(metadataPartStorageJsonWithNotifications(t, ""))
	assert.Nil(t, err)
	_, ok := store.(*notification.StorageMiddleware)
	assert.True(t, ok, "notifications should be enabled by default")
}

func TestMetadataPartStorageNotificationsCanBeDisabled(t *testing.T) {
	testutils.SkipIfIntegration(t)

	store, err := createStorageFromJson(metadataPartStorageJsonWithNotifications(t, `,"notifications": {"enabled": false}`))
	assert.Nil(t, err)
	_, ok := store.(*notification.StorageMiddleware)
	assert.False(t, ok, "notifications should be disabled when enabled is false")
}

func TestMetadataPartStorageNotificationsAcceptCustomConfig(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fragment := `,"notifications": {` +
		`"notificationDestinations": {"arn:aws:sqs:eu-central-1:000000000000:queue": {"type": "aws"}},` +
		`"maxAttempts": 5, "minBackoffSeconds": 1, "maxBackoffSeconds": 300, "dispatcherConcurrency": 4, "batchSize": 10}`
	store, err := createStorageFromJson(metadataPartStorageJsonWithNotifications(t, fragment))
	assert.Nil(t, err)
	_, ok := store.(*notification.StorageMiddleware)
	assert.True(t, ok, "a custom notifications block should still enable notifications")
}

func TestMetadataPartStorageNotificationsRejectInvalidDispatcherConfig(t *testing.T) {
	testutils.SkipIfIntegration(t)

	_, err := createStorageFromJson(metadataPartStorageJsonWithNotifications(t, `,"notifications": {"batchSize": 0}`))
	assert.NotNil(t, err)
}

func metadataPartStorageJsonWithExtraPartStores(t *testing.T, extraPartStoresAndMapping string) []byte {
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	return fmt.Appendf(nil, `{
			"type": "MetadataPartStorage",
			"db": {
				"type": "RegisterDatabaseReference",
				"refName": "db",
				"db": {
					"type": "SqliteDatabase",
					"dbPath": %s
				}
			},
			"metadataStore": {
				"type": "SqlMetadataStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				}
			},
			"partStore": {
				"type": "FilesystemPartStore",
				"root": %s
			}%s
		}`, strconv.Quote(dbPath), strconv.Quote(storagePath), extraPartStoresAndMapping)
}

func TestCanCreateMetadataPartStorageWithExtraPartStoresFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	coldDir, coldCleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(coldCleanup)

	jsonData := metadataPartStorageJsonWithExtraPartStores(t, fmt.Sprintf(`,
			"extraPartStores": {
				"cold": {
					"type": "FilesystemPartStore",
					"root": %s
				}
			},
			"storageClassToPartStore": {
				"GLACIER": "cold",
				"DEEP_ARCHIVE": "cold",
				"STANDARD_IA": "default"
			}`, strconv.Quote(*coldDir)))

	storage, err := createStorageFromJson(jsonData)
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestMetadataPartStorageRejectsReservedExtraPartStoreName(t *testing.T) {
	testutils.SkipIfIntegration(t)

	coldDir, coldCleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(coldCleanup)

	jsonData := metadataPartStorageJsonWithExtraPartStores(t, fmt.Sprintf(`,
			"extraPartStores": {
				"default": {
					"type": "FilesystemPartStore",
					"root": %s
				}
			}`, strconv.Quote(*coldDir)))

	_, err = createStorageFromJson(jsonData)
	assert.ErrorContains(t, err, "reserved name")
}

func TestMetadataPartStorageRejectsUnknownStorageClassInMapping(t *testing.T) {
	testutils.SkipIfIntegration(t)

	jsonData := metadataPartStorageJsonWithExtraPartStores(t, `,
			"storageClassToPartStore": {
				"FROZEN_SOLID": "default"
			}`)

	_, err := createStorageFromJson(jsonData)
	assert.ErrorContains(t, err, "not a recognized storage class")
}

func TestMetadataPartStorageRejectsMappingToUnknownPartStore(t *testing.T) {
	testutils.SkipIfIntegration(t)

	jsonData := metadataPartStorageJsonWithExtraPartStores(t, `,
			"storageClassToPartStore": {
				"GLACIER": "cold"
			}`)

	_, err := createStorageFromJson(jsonData)
	assert.ErrorContains(t, err, "unknown part store")
}

func TestMetadataPartStorageRejectsSharedSqlPartStoresWithoutExplicitIds(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	dbPath := filepath.Join(*tempDir, "pithos.db")
	jsonData := fmt.Sprintf(`{
			"type": "MetadataPartStorage",
			"db": {
				"type": "RegisterDatabaseReference",
				"refName": "db",
				"db": {
					"type": "SqliteDatabase",
					"dbPath": %s
				}
			},
			"metadataStore": {
				"type": "SqlMetadataStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				}
			},
			"partStore": {
				"type": "SqlPartStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				}
			},
			"extraPartStores": {
				"cold": {
					"type": "SqlPartStore",
					"db": {
						"type": "DatabaseReference",
						"refName": "db"
					}
				}
			},
			"storageClassToPartStore": {
				"GLACIER": "cold"
			}
		}`, strconv.Quote(dbPath))

	_, err = createStorageFromJson([]byte(jsonData))
	assert.ErrorContains(t, err, "must set partStoreId")
}

func TestMetadataPartStorageAllowsSharedSqlPartStoresWithUniqueExplicitIds(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	dbPath := filepath.Join(*tempDir, "pithos.db")
	jsonData := fmt.Sprintf(`{
			"type": "MetadataPartStorage",
			"db": {
				"type": "RegisterDatabaseReference",
				"refName": "db",
				"db": {
					"type": "SqliteDatabase",
					"dbPath": %s
				}
			},
			"metadataStore": {
				"type": "SqlMetadataStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				}
			},
			"partStore": {
				"type": "SqlPartStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				},
				"partStoreId": "hot"
			},
			"extraPartStores": {
				"cold": {
					"type": "SqlPartStore",
					"db": {
						"type": "DatabaseReference",
						"refName": "db"
					},
					"partStoreId": "cold"
				}
			},
			"storageClassToPartStore": {
				"GLACIER": "cold"
			}
		}`, strconv.Quote(dbPath))

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
			"type": "MetadataPartStorage",
			"db": {
				"type": "RegisterDatabaseReference",
				"refName": "db",
				"db": {
					"type": "SqliteDatabase",
					"dbPath": %s
				}
			},
			"metadataStore": {
				"type": "SqlMetadataStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				}
			},
			"partStore": {
				"type": "FilesystemPartStore",
				"root": %s
			}
		}
	  },
	  "defaultStorage": {
			"type": "MetadataPartStorage",
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
			"partStore": {
				"type": "SqlPartStore",
				"db": {
					"type": "DatabaseReference",
					"refName": "db"
				}
			}
		}
	}`, strconv.Quote(dbPath), strconv.Quote(storagePath))

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateObjectCacheStorageMiddlewareFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
			"type": "ObjectCacheStorageMiddleware",
			"maxObjectSizeBytes": 1048576,
			"cacheReadErrorsAsMiss": true,
			"cache": {
				"type": "GenericCache",
				"cachePersistor": {
					"type": "InMemoryPersistor"
				},
				"cacheEvictionPolicy": {
					"type": "EvictNothingEvictionPolicy"
				}
			},
			"innerStorage": {
				"type": "MetadataPartStorage",
				"db": {
					"type": "RegisterDatabaseReference",
					"refName": "db",
					"db": {
						"type": "SqliteDatabase",
						"dbPath": %s
					}
				},
				"metadataStore": {
					"type": "SqlMetadataStore",
					"db": {
						"type": "DatabaseReference",
						"refName": "db"
					}
				},
				"partStore": {
					"type": "FilesystemPartStore",
					"root": %s
				}
			}
		}`, strconv.Quote(dbPath), strconv.Quote(storagePath))

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
					"dbPath": %s
				}
			},
			"innerStorage": {
				"type": "MetadataPartStorage",
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
				"partStore": {
					"type": "FilesystemPartStore",
					"root": %s
				}
			}
		}`, strconv.Quote(dbPath), strconv.Quote(storagePath))

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
				"type": "MetadataPartStorage",
				"db": {
					"type": "RegisterDatabaseReference",
					"refName": "db",
					"db": {
						"type": "SqliteDatabase",
						"dbPath": %s
					}
				},
				"metadataStore": {
					"type": "SqlMetadataStore",
					"db": {
						"type": "DatabaseReference",
						"refName": "db"
					}
				},
				"partStore": {
					"type": "FilesystemPartStore",
					"root": %s
				}
			},
			"secondaryStorages": []
		}`, strconv.Quote(dbPath), strconv.Quote(storagePath))

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
				"type": "MetadataPartStorage",
				"db": {
					"type": "RegisterDatabaseReference",
					"refName": "db",
					"db": {
						"type": "SqliteDatabase",
						"dbPath": %s
					}
				},
				"metadataStore": {
					"type": "SqlMetadataStore",
					"db": {
						"type": "DatabaseReference",
						"refName": "db"
					}
				},
				"partStore": {
					"type": "FilesystemPartStore",
					"root": %s
				}
			},
			"secondaryStorages": [
				{
					"type": "MetadataPartStorage",
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
					"partStore": {
						"type": "FilesystemPartStore",
						"root": %s
					}
				}
			]
		}`, strconv.Quote(dbPath), strconv.Quote(storagePath), strconv.Quote(storagePath))

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateAuditStorageMiddlewareFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	auditLogPath := filepath.Join(storagePath, "audit.log")

	// Generate keys for signing
	_, edPriv, _ := ed25519.GenerateKey(rand.Reader)
	edPrivEncoded := base64.StdEncoding.EncodeToString(edPriv)

	mlPriv, _ := mldsa.GenerateKey(mldsa.MLDSA87())
	mlPrivBytes := mlPriv.Bytes()
	mlPrivEncoded := base64.StdEncoding.EncodeToString(mlPrivBytes)

	jsonData := fmt.Sprintf(`{
			"type": "AuditStorageMiddleware",
			"innerStorage": {
				"type": "MetadataPartStorage",
				"db": {
					"type": "RegisterDatabaseReference",
					"refName": "db",
					"db": {
						"type": "SqliteDatabase",
						"dbPath": %s
					}
				},
				"metadataStore": {
					"type": "SqlMetadataStore",
					"db": {
						"type": "DatabaseReference",
						"refName": "db"
					}
				},
				"partStore": {
					"type": "FilesystemPartStore",
					"root": %s
				}
			},
			"sinks": [
				{
					"type": "FileSink",
					"path": %s,
					"serializer": {
						"type": "BinarySerializer"
					}
				}
			],
			"signing": {
				"ed25519": {
					"privateKey": %s
				},
				"mlDsa87": {
					"privateKey": %s
				}
			}
		}`, strconv.Quote(dbPath), strconv.Quote(storagePath), strconv.Quote(auditLogPath), strconv.Quote(edPrivEncoded), strconv.Quote(mlPrivEncoded))

	storage, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, storage)
}

func TestCanCreateAuditStorageMiddlewareWithMultipleSinksFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	auditLog1 := filepath.Join(storagePath, "audit1.log")
	auditLog2 := filepath.Join(storagePath, "audit2.log")

	// Generate keys for signing
	_, edPriv, _ := ed25519.GenerateKey(rand.Reader)
	edPrivEncoded := base64.StdEncoding.EncodeToString(edPriv)

	mlPriv, _ := mldsa.GenerateKey(mldsa.MLDSA87())
	mlPrivBytes := mlPriv.Bytes()
	mlPrivEncoded := base64.StdEncoding.EncodeToString(mlPrivBytes)

	jsonData := fmt.Sprintf(`{
			"type": "AuditStorageMiddleware",
			"innerStorage": {
				"type": "MetadataPartStorage",
				"db": {
					"type": "RegisterDatabaseReference",
					"refName": "db",
					"db": {
						"type": "SqliteDatabase",
						"dbPath": %s
					}
				},
				"metadataStore": {
					"type": "SqlMetadataStore",
					"db": {
						"type": "DatabaseReference",
						"refName": "db"
					}
				},
				"partStore": {
					"type": "FilesystemPartStore",
					"root": %s
				}
			},
			"sinks": [
				{
					"type": "FileSink",
					"path": %s,
					"serializer": {
						"type": "BinarySerializer"
					}
				},
				{
					"type": "FileSink",
					"path": %s,
					"serializer": {
						"type": "TextSerializer"
					}
				}
			],
			"signing": {
				"ed25519": {
					"privateKey": %s
				},
				"mlDsa87": {
					"privateKey": %s
				}
			}
		}`, strconv.Quote(dbPath), strconv.Quote(storagePath), strconv.Quote(auditLog1), strconv.Quote(auditLog2), strconv.Quote(edPrivEncoded), strconv.Quote(mlPrivEncoded))

	s, err := createStorageFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, s)
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

func TestSigningVerificationKeys(t *testing.T) {
	testutils.SkipIfIntegration(t)
	edPub, edPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	mlPriv, err := mldsa.GenerateKey(mldsa.MLDSA87())
	require.NoError(t, err)
	encode := base64.StdEncoding.EncodeToString

	derived := &SigningConfiguration{Ed25519: &Ed25519SigningConfiguration{PrivateKey: encode(edPriv)}, MlDsa87: &MlDsa87SigningConfiguration{PrivateKey: encode(mlPriv.Bytes())}}
	ed, ml, err := derived.VerificationKeys()
	require.NoError(t, err)
	require.True(t, ed.Equal(edPub))
	require.True(t, ml.Equal(mlPriv.PublicKey()))

	_, otherEdPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pinned := &SigningConfiguration{
		Ed25519: &Ed25519SigningConfiguration{Vault: &VaultSigningConfiguration{Address: "https://vault", KeyPath: "transit/key"}, PublicKey: encode(edPub)},
		MlDsa87: &MlDsa87SigningConfiguration{PrivateKey: encode(mlPriv.Bytes()), PublicKey: encode(mlPriv.PublicKey().Bytes())},
	}
	ed, _, err = pinned.VerificationKeys()
	require.NoError(t, err)
	require.True(t, ed.Equal(edPub), "a pinned public key verifies Vault-held keys")

	vaultOnly := &SigningConfiguration{Ed25519: &Ed25519SigningConfiguration{Vault: &VaultSigningConfiguration{Address: "https://vault", KeyPath: "transit/key"}, PrivateKey: encode(otherEdPriv)}, MlDsa87: derived.MlDsa87}
	ed, ml, err = vaultOnly.VerificationKeys()
	require.NoError(t, err)
	require.Nil(t, ed, "Vault keys without a public key cannot be verified locally")
	require.NotNil(t, ml)

	_, _, err = (&SigningConfiguration{}).VerificationKeys()
	require.Error(t, err)
}

func TestAuditSigningRejectsMismatchedPublicKeys(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	_, edPriv, _ := ed25519.GenerateKey(rand.Reader)
	otherEdPub, _, _ := ed25519.GenerateKey(rand.Reader)
	mlPriv, _ := mldsa.GenerateKey(mldsa.MLDSA87())
	otherMl, _ := mldsa.GenerateKey(mldsa.MLDSA87())
	encode := base64.StdEncoding.EncodeToString
	build := func(edPublic, mlPublic string) []byte {
		return []byte(fmt.Sprintf(`{"type":"AuditStorageMiddleware",
			"innerStorage":{"type":"MetadataPartStorage","db":{"type":"RegisterDatabaseReference","refName":"db","db":{"type":"SqliteDatabase","dbPath":%s}},
				"metadataStore":{"type":"SqlMetadataStore","db":{"type":"DatabaseReference","refName":"db"}},"partStore":{"type":"FilesystemPartStore","root":%s}},
			"sinks":[{"type":"FileSink","path":%s,"serializer":{"type":"BinarySerializer"}}],
			"signing":{"ed25519":{"privateKey":%s,"publicKey":%s},"mlDsa87":{"privateKey":%s,"publicKey":%s}}}`,
			strconv.Quote(filepath.Join(*tempDir, "pithos.db")), strconv.Quote(*tempDir), strconv.Quote(filepath.Join(*tempDir, "audit.log")),
			strconv.Quote(encode(edPriv)), strconv.Quote(edPublic), strconv.Quote(encode(mlPriv.Bytes())), strconv.Quote(mlPublic)))
	}
	_, err = createStorageFromJson(build(encode(otherEdPub), encode(mlPriv.PublicKey().Bytes())))
	require.ErrorContains(t, err, "signing.ed25519.publicKey does not match")
	_, err = createStorageFromJson(build(encode(edPriv.Public().(ed25519.PublicKey)), encode(otherMl.PublicKey().Bytes())))
	require.ErrorContains(t, err, "signing.mlDsa87.publicKey does not match")
	storage, err := createStorageFromJson(build(encode(edPriv.Public().(ed25519.PublicKey)), encode(mlPriv.PublicKey().Bytes())))
	require.NoError(t, err)
	require.NotNil(t, storage)
}

func TestAuditSigningRejectsMismatchedVaultPublicKey(t *testing.T) {
	testutils.SkipIfIntegration(t)
	vaultPub, vaultPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	otherPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	vault := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input string `json:"input"`
		}
		if r.URL.Path != "/v1/transit/sign/audit" || json.NewDecoder(r.Body).Decode(&body) != nil {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		input, err := base64.StdEncoding.DecodeString(body.Input)
		if err != nil {
			http.Error(w, "bad input", http.StatusBadRequest)
			return
		}
		signature := base64.StdEncoding.EncodeToString(ed25519.Sign(vaultPriv, input))
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"signature": "vault:v1:" + signature}})
	}))
	t.Cleanup(vault.Close)

	tempDir, cleanup, err := config.CreateTempDir()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	mlPriv, err := mldsa.GenerateKey(mldsa.MLDSA87())
	require.NoError(t, err)
	encode := base64.StdEncoding.EncodeToString
	build := func(edPublic string) []byte {
		return []byte(fmt.Sprintf(`{"type":"AuditStorageMiddleware",
			"innerStorage":{"type":"MetadataPartStorage","db":{"type":"RegisterDatabaseReference","refName":"db","db":{"type":"SqliteDatabase","dbPath":%s}},
				"metadataStore":{"type":"SqlMetadataStore","db":{"type":"DatabaseReference","refName":"db"}},"partStore":{"type":"FilesystemPartStore","root":%s}},
			"sinks":[{"type":"FileSink","path":%s,"serializer":{"type":"BinarySerializer"}}],
			"signing":{"ed25519":{"vault":{"address":%s,"token":"test-token","keyPath":"transit/audit"},"publicKey":%s},"mlDsa87":{"privateKey":%s}}}`,
			strconv.Quote(filepath.Join(*tempDir, "pithos.db")), strconv.Quote(*tempDir), strconv.Quote(filepath.Join(*tempDir, "audit.log")),
			strconv.Quote(vault.URL), strconv.Quote(edPublic), strconv.Quote(encode(mlPriv.Bytes()))))
	}
	_, err = createStorageFromJson(build(encode(otherPub)))
	require.ErrorContains(t, err, "signing.ed25519.publicKey does not match the Vault signing key")
	storage, err := createStorageFromJson(build(encode(vaultPub)))
	require.NoError(t, err)
	require.NotNil(t, storage)
}
