package config

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func createMetadataStoreFromJson(b []byte) (metadatastore.MetadataStore, error) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		return nil, err
	}
	dbContainer := config.NewDbContainer()
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*config.DbContainer)(nil)), dbContainer)
	if err != nil {
		return nil, err
	}
	mi, err := CreateMetadataStoreInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	err = mi.RegisterReferences(diContainer)
	if err != nil {
		return nil, err
	}
	return mi.Instantiate(diContainer)
}

func TestCanCreateSqlMetadataStoreFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
	  "type": "SqlMetadataStore",
	  "db": {
	    "type": "SqliteDatabase",
		"dbPath": %s
	  }
	}`, strconv.Quote(dbPath))

	metadataStore, err := createMetadataStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, metadataStore)
}
