package config

import (
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor"
	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor/inmemory"
)

const (
	FilesystemPersistorType = "FilesystemPersistor"
	InMemoryPersistorType   = "InMemoryPersistor"
)

type CachePersistorInstantiator = internalConfig.DynamicJsonInstantiator[persistor.CachePersistor]

type FilesystemPersistorConfiguration struct {
	Root string `json:"root"`
	internalConfig.DynamicJsonType
}

func (c *FilesystemPersistorConfiguration) Instantiate(diContainer dependencyinjection.DIContainer) (persistor.CachePersistor, error) {
	return filesystem.New(c.Root)
}

type InMemoryPersistorConfiguration struct {
	internalConfig.DynamicJsonType
}

func (c *InMemoryPersistorConfiguration) Instantiate(diContainer dependencyinjection.DIContainer) (persistor.CachePersistor, error) {
	return inmemory.New()
}

func CreateCachePersistorInstantiatorFromJson(b []byte) (CachePersistorInstantiator, error) {
	var cpc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &cpc)
	if err != nil {
		return nil, err
	}

	var cpi CachePersistorInstantiator
	switch cpc.Type {
	case FilesystemPersistorType:
		cpi = &FilesystemPersistorConfiguration{}
	case InMemoryPersistorType:
		cpi = &InMemoryPersistorConfiguration{}
	default:
		return nil, errors.New("unknown cachePersistor type")
	}
	err = json.Unmarshal(b, &cpi)
	if err != nil {
		return nil, err
	}
	return cpi, nil
}
