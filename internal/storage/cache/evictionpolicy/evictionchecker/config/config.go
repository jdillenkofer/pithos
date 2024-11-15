package config

import (
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker/fixedkeylimit"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker/fixedsizelimit"
)

const (
	fixedKeyLimitType  = "FixedKeyLimit"
	fixedSizeLimitType = "FixedSizeLimit"
)

type EvictionCheckerInstantiator = internalConfig.DynamicJsonInstantiator[evictionchecker.EvictionChecker]

type FixedKeyLimitEvictionCheckerConfiguration struct {
	MaxKeyLimit internalConfig.Int64Provider `json:"maxKeyLimit"`
	internalConfig.DynamicJsonType
}

func (f *FixedKeyLimitEvictionCheckerConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (f *FixedKeyLimitEvictionCheckerConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (evictionchecker.EvictionChecker, error) {
	return fixedkeylimit.New(int(f.MaxKeyLimit.Value()))
}

type FixedSizeLimitEvictionCheckerConfiguration struct {
	MaxSizeLimit internalConfig.Int64Provider `json:"maxSizeLimit"`
	internalConfig.DynamicJsonType
}

func (f *FixedSizeLimitEvictionCheckerConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (f *FixedSizeLimitEvictionCheckerConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (evictionchecker.EvictionChecker, error) {
	return fixedsizelimit.New(f.MaxSizeLimit.Value())
}

func CreateEvictionCheckerInstantiatorFromJson(b []byte) (EvictionCheckerInstantiator, error) {
	var ecc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &ecc)
	if err != nil {
		return nil, err
	}

	var eci EvictionCheckerInstantiator
	switch ecc.Type {
	case fixedKeyLimitType:
		eci = &FixedKeyLimitEvictionCheckerConfiguration{}
	case fixedSizeLimitType:
		eci = &FixedSizeLimitEvictionCheckerConfiguration{}
	default:
		return nil, errors.New("unknown evictionChecker type")
	}
	err = json.Unmarshal(b, &eci)
	if err != nil {
		return nil, err
	}
	return eci, nil
}
