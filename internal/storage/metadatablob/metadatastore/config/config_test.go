package config

import (
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
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
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "SqlMetadataStore",
	  "db": {
	    "type": "SqliteDatabase",
		"dbPath": "/tmp/pithos/pithos.db"
	  }
	}`
	metadataStore, err := createMetadataStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, metadataStore)
}

func TestCanCreateTracingMetadataStoreFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "TracingMetadataStoreMiddleware",
	  "innerMetadataStore": {
	    "type": "SqlMetadataStore",
	    "db": {
	      "type": "SqliteDatabase",
		  "dbPath": "/tmp/pithos/pithos.db"
	    }
	  }
	}`
	metadataStore, err := createMetadataStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, metadataStore)
}
