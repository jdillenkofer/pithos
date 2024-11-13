package config

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	"github.com/stretchr/testify/assert"
)

func createMetadataStoreFromJson(b []byte) (metadatastore.MetadataStore, error) {
	diContainer, err := dependencyinjection.NewContainer()
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
	jsonData := `{
	  "type": "SqlMetadataStore",
	  "db": {
	    "type": "SqliteDatabase",
		"storagePath": "/tmp/pithos/"
	  }
	}`
	metadataStore, err := createMetadataStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, metadataStore)
}

func TestCanCreateTracingMetadataStoreFromJson(t *testing.T) {
	jsonData := `{
	  "type": "TracingMetadataStoreMiddleware",
	  "innerMetadataStore": {
	    "type": "SqlMetadataStore",
	    "db": {
	      "type": "SqliteDatabase",
		  "storagePath": "/tmp/pithos/"
	    }
	  }
	}`
	metadataStore, err := createMetadataStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, metadataStore)
}
