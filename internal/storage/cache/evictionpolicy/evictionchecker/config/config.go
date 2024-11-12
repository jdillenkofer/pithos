package config

import (
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker/fixedkeylimit"
	"github.com/jdillenkofer/pithos/internal/storage/cache/evictionpolicy/evictionchecker/fixedsizelimit"
)

const (
	FixedKeyLimitType  = "FixedKeyLimit"
	FixedSizeLimitType = "FixedSizeLimit"
)

type EvictionCheckerInstantiator = internalConfig.DynamicJsonInstantiator[evictionchecker.EvictionChecker]

type FixedKeyLimitEvictionCheckerConfiguration struct {
	MaxKeyLimit int `json:"maxKeyLimit"`
	internalConfig.DynamicJsonType
}

func (f *FixedKeyLimitEvictionCheckerConfiguration) Instantiate() (evictionchecker.EvictionChecker, error) {
	return fixedkeylimit.New(f.MaxKeyLimit)
}

type FixedSizeLimitEvictionCheckerConfiguration struct {
	MaxSizeLimit int64 `json:"maxSizeLimit"`
	internalConfig.DynamicJsonType
}

func (f *FixedSizeLimitEvictionCheckerConfiguration) Instantiate() (evictionchecker.EvictionChecker, error) {
	return fixedsizelimit.New(f.MaxSizeLimit)
}

func CreateEvictionCheckerInstantiatorFromJson(b []byte) (EvictionCheckerInstantiator, error) {
	var ecc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &ecc)
	if err != nil {
		return nil, err
	}

	var eci EvictionCheckerInstantiator
	switch ecc.Type {
	case FixedKeyLimitType:
		eci = &FixedKeyLimitEvictionCheckerConfiguration{}
	case FixedSizeLimitType:
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
