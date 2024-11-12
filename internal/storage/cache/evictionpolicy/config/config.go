package config

import (
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker/config"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictnothing"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/lfu"
)

const (
	LFUEvictionPolicyType          = "LFUEvictionPolicy"
	EvictNothingEvictionPolicyType = "EvictNothingEvictionPolicy"
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

func (l *LFUEvictionPolicyConfiguration) Instantiate() (evictionpolicy.CacheEvictionPolicy, error) {
	evictionChecker, err := l.EvictionCheckerInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	return lfu.New(evictionChecker)
}

type EvictNothingEvictionPolicyConfiguration struct {
	internalConfig.DynamicJsonType
}

func (*EvictNothingEvictionPolicyConfiguration) Instantiate() (evictionpolicy.CacheEvictionPolicy, error) {
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
	case LFUEvictionPolicyType:
		cepi = &LFUEvictionPolicyConfiguration{}
	case EvictNothingEvictionPolicyType:
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
