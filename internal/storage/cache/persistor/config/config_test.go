package config

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor"
	"github.com/stretchr/testify/assert"
)

func createCachePersistorFromJson(b []byte) (persistor.CachePersistor, error) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		return nil, err
	}
	dbContainer := config.NewDbContainer()
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*config.DbContainer)(nil)), dbContainer)
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
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
	  "type": "FilesystemPersistor",
	  "root": "%v"
	}`, storagePath)

	cachePersistor, err := createCachePersistorFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, cachePersistor)
}
