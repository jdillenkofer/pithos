package config

import (
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/cache"
	evictionPolicyConfig "github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/config"
	persistorConfig "github.com/jdillenkofer/pithos/internal/storage/cache/persistor/config"
)

const (
	GenericCacheType = "GenericCache"
)

type CacheInstantiator = internalConfig.DynamicJsonInstantiator[cache.Cache]

type GenericCacheConfiguration struct {
	CachePersistorInstantiator      persistorConfig.CachePersistorInstantiator           `json:"-"`
	RawCachePersistor               json.RawMessage                                      `json:"cachePesistor"`
	CacheEvictionPolicyInstantiator evictionPolicyConfig.CacheEvictionPolicyInstantiator `json:"-"`
	RawCacheEvictionPolicy          json.RawMessage                                      `json:"cacheEvictionPolicy"`
	internalConfig.DynamicJsonType
}

func (c *GenericCacheConfiguration) UnmarshalJSON(b []byte) error {
	type genericCacheConfiguration GenericCacheConfiguration
	err := json.Unmarshal(b, (*genericCacheConfiguration)(c))
	if err != nil {
		return err
	}
	c.CachePersistorInstantiator, err = persistorConfig.CreateCachePersistorInstantiatorFromJson(c.RawCachePersistor)
	if err != nil {
		return err
	}
	c.CacheEvictionPolicyInstantiator, err = evictionPolicyConfig.CreateCacheEvictionPolicyInstantiatorFromJson(c.RawCacheEvictionPolicy)
	if err != nil {
		return err
	}
	return nil
}

func (c *GenericCacheConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (cache.Cache, error) {
	cachePersistor, err := c.CachePersistorInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	cacheEvictionPolicy, err := c.CacheEvictionPolicyInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return cache.NewGenericCache(cachePersistor, cacheEvictionPolicy)
}

func CreateCacheInstantiatorFromJson(b []byte) (CacheInstantiator, error) {
	var cc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &cc)
	if err != nil {
		return nil, err
	}

	var ci CacheInstantiator
	switch cc.Type {
	case GenericCacheType:
		ci = &GenericCacheConfiguration{}
	default:
		return nil, errors.New("unknown cache type")
	}
	err = json.Unmarshal(b, &ci)
	if err != nil {
		return nil, err
	}
	return ci, nil
}
