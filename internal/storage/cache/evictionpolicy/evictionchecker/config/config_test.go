package config

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker"
	"github.com/stretchr/testify/assert"
)

func createEvictionCheckerFromJson(b []byte) (evictionchecker.EvictionChecker, error) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		return nil, err
	}
	si, err := CreateEvictionCheckerInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	err = si.RegisterReferences(diContainer)
	if err != nil {
		return nil, err
	}
	return si.Instantiate(diContainer)
}

func TestCanCreateFixedKeyLimitEvictionCheckerFromJson(t *testing.T) {
	jsonData := `{
	  "type": "FixedKeyLimit",
	  "maxKeyLimit": 5
	}`
	evictionChecker, err := createEvictionCheckerFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, evictionChecker)
}

func TestCanCreateFixedSizeLimitEvictionCheckerFromJson(t *testing.T) {
	jsonData := `{
	  "type": "FixedSizeLimit",
	  "maxSizeLimit": 5
	}`
	evictionChecker, err := createEvictionCheckerFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, evictionChecker)
}
