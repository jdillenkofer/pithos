package migrator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage"
	storageConfig "github.com/jdillenkofer/pithos/internal/storage/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
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
	si, err := storageConfig.CreateStorageInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	err = si.RegisterReferences(diContainer)
	if err != nil {
		return nil, err
	}
	return si.Instantiate(diContainer)
}

func TestStorageMigrator(t *testing.T) {
	// Arrange
	const bucketName = "test"
	const objectKey = "zzz"
	var objectData = []byte("abc")
	ctx := context.Background()

	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)

	tempDir2, cleanup2, err := config.CreateTempDir()
	assert.Nil(t, err)

	cleanupTempDirs := func() {
		cleanup()
		cleanup2()
	}

	t.Cleanup(cleanupTempDirs)

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
	storage.Start(ctx)
	defer storage.Stop(ctx)

	err = storage.CreateBucket(ctx, bucketName)
	assert.Nil(t, err)

	err = storage.PutObject(ctx, bucketName, objectKey, bytes.NewReader(objectData))
	assert.Nil(t, err)

	storagePath2 := *tempDir2
	dbPath2 := filepath.Join(storagePath2, "pithos.db")
	jsonData2 := fmt.Sprintf(`{
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
	    "type": "SqlBlobStore",
		"db": {
	      "type": "DatabaseReference",
		  "refName": "db"
	    }
	  }
	}`, dbPath2)

	storage2, err := createStorageFromJson([]byte(jsonData2))
	assert.Nil(t, err)
	storage2.Start(ctx)
	defer storage2.Stop(ctx)

	// Act
	err = MigrateStorage(ctx, storage, storage2)
	assert.Nil(t, err)

	// Assert
	buckets, err := storage2.ListBuckets(ctx)
	assert.Nil(t, err)
	assert.Equal(t, bucketName, buckets[0].Name)
	reader, err := storage2.GetObject(ctx, bucketName, objectKey, nil, nil)
	assert.Nil(t, err)
	data, err := io.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, objectData, data)
}
