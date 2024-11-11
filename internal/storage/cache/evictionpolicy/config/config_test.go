package config

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy"
	"github.com/stretchr/testify/assert"
)

func createCacheEvictionPolicyFromJson(b []byte) (evictionpolicy.CacheEvictionPolicy, error) {
	si, err := CreateCacheEvictionPolicyInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	return si.Instantiate()
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
