package config

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	"github.com/stretchr/testify/assert"
)

func createMetadataStoreFromJson(b []byte) (metadatastore.MetadataStore, error) {
	mi, err := CreateMetadataStoreInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	return mi.Instantiate()
}

func TestCanCreateSqlMetadataStoreFromJson(t *testing.T) {
	jsonData := `{
	  "type": "SqlMetadataStore"
	}`
	metadataStore, err := createMetadataStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, metadataStore)
}

func TestCanCreateTracingMetadataStoreFromJson(t *testing.T) {
	jsonData := `{
	  "type": "TracingMetadataStoreMiddleware",
	  "innerMetadataStore": {
	    "type": "SqlMetadataStore"
	  }
	}`
	metadataStore, err := createMetadataStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, metadataStore)
}
