package config

import (
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker/config"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictnothing"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/lfu"
)

const (
	lfuEvictionPolicyType          = "LFUEvictionPolicy"
	evictNothingEvictionPolicyType = "EvictNothingEvictionPolicy"
)

type CacheEvictionPolicyInstantiator = internalConfig.DynamicJsonInstantiator[evictionpolicy.CacheEvictionPolicy]

type LFUEvictionPolicyConfiguration struct {
	EvictionCheckerInstantiator config.EvictionCheckerInstantiator `json:"-"`
	RawEvictionChecker          json.RawMessage                    `json:"evictionChecker"`
	internalConfig.DynamicJsonType
}

func (c *LFUEvictionPolicyConfiguration) UnmarshalJSON(b []byte) error {
	type lFUEvictionPolicyConfiguration LFUEvictionPolicyConfiguration
	err := json.Unmarshal(b, (*lFUEvictionPolicyConfiguration)(c))
	if err != nil {
		return err
	}
	c.EvictionCheckerInstantiator, err = config.CreateEvictionCheckerInstantiatorFromJson(c.RawEvictionChecker)
	if err != nil {
		return err
	}
	return nil
}

func (l *LFUEvictionPolicyConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := l.EvictionCheckerInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (l *LFUEvictionPolicyConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (evictionpolicy.CacheEvictionPolicy, error) {
	evictionChecker, err := l.EvictionCheckerInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return lfu.New(evictionChecker)
}

type EvictNothingEvictionPolicyConfiguration struct {
	internalConfig.DynamicJsonType
}

func (*EvictNothingEvictionPolicyConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (*EvictNothingEvictionPolicyConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (evictionpolicy.CacheEvictionPolicy, error) {
	return evictnothing.New()
}

func CreateCacheEvictionPolicyInstantiatorFromJson(b []byte) (CacheEvictionPolicyInstantiator, error) {
	var epc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &epc)
	if err != nil {
		return nil, err
	}

	var cepi CacheEvictionPolicyInstantiator
	switch epc.Type {
	case lfuEvictionPolicyType:
		cepi = &LFUEvictionPolicyConfiguration{}
	case evictNothingEvictionPolicyType:
		cepi = &EvictNothingEvictionPolicyConfiguration{}
	default:
		return nil, errors.New("unknown cacheEvictionPolicy type")
	}
	err = json.Unmarshal(b, &cepi)
	if err != nil {
		return nil, err
	}
	return cepi, nil
}
