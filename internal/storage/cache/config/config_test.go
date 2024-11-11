package config

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/cache"
	"github.com/stretchr/testify/assert"
)

func createCacheFromJson(b []byte) (cache.Cache, error) {
	ci, err := CreateCacheInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	return ci.Instantiate()
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
