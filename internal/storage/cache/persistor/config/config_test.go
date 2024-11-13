package config

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor"
	"github.com/stretchr/testify/assert"
)

func createCachePersistorFromJson(b []byte) (persistor.CachePersistor, error) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		return nil, err
	}
	ci, err := CreateCachePersistorInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	err = ci.RegisterReferences(diContainer)
	if err != nil {
		return nil, err
	}
	return ci.Instantiate(diContainer)
}

func TestCanCreateInMemoryPersistorFromJson(t *testing.T) {
	jsonData := `{
	  "type": "InMemoryPersistor"
	}`
	cachePersistor, err := createCachePersistorFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, cachePersistor)
}

func TestCanCreateFilesystemPersistorFromJson(t *testing.T) {
	// @TODO: Generate tmpDir dynamically and delete after test
	jsonData := `{
	  "type": "FilesystemPersistor",
	  "root": "/tmp/pithos/"
	}`
	cachePersistor, err := createCachePersistorFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, cachePersistor)
}
