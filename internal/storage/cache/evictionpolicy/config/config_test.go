package config

import (
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy"
	"github.com/stretchr/testify/assert"
)

func createCacheEvictionPolicyFromJson(b []byte) (evictionpolicy.CacheEvictionPolicy, error) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		return nil, err
	}
	dbContainer := config.NewDbContainer()
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*config.DbContainer)(nil)), dbContainer)
	if err != nil {
		return nil, err
	}
	si, err := CreateCacheEvictionPolicyInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	err = si.RegisterReferences(diContainer)
	if err != nil {
		return nil, err
	}
	return si.Instantiate(diContainer)
}

func TestCanCreateEvictNothingEvictionPolicyFromJson(t *testing.T) {
	jsonData := `{
	  "type": "EvictNothingEvictionPolicy"
	}`
	evictionPolicy, err := createCacheEvictionPolicyFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, evictionPolicy)
}

func TestCanCreateLFUEvictionPolicyFromJson(t *testing.T) {
	jsonData := `{
	  "type": "LFUEvictionPolicy",
	  "evictionChecker": {
	    "type": "FixedKeyLimit",
		"maxKeyLimit": 5
	  }
	}`
	evictionPolicy, err := createCacheEvictionPolicyFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, evictionPolicy)
}
