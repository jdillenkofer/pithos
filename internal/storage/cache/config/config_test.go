package config

import (
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/cache"
	"github.com/stretchr/testify/assert"
)

func createCacheFromJson(b []byte) (cache.Cache, error) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		return nil, err
	}
	dbContainer := config.NewDbContainer()
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*config.DbContainer)(nil)), dbContainer)
	if err != nil {
		return nil, err
	}
	ci, err := CreateCacheInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	err = ci.RegisterReferences(diContainer)
	if err != nil {
		return nil, err
	}
	return ci.Instantiate(diContainer)
}

func TestCanCreateCacheFromJson(t *testing.T) {
	jsonData := `{
	  "type": "GenericCache",
	  "cachePesistor": {
	    "type": "InMemoryPersistor"
	  },
	  "cacheEvictionPolicy": {
	    "type": "EvictNothingEvictionPolicy"
	  }
	}`
	cache, err := createCacheFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, cache)
}
