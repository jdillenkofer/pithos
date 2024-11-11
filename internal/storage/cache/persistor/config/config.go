package config

import (
	"encoding/json"
	"errors"

	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor"
	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor/inmemory"
)

const (
	FilesystemPersistorType = "FilesystemPersistor"
	InMemoryPersistorType   = "InMemoryPersistor"
)

type CachePersistorInstantiator interface {
	Instantiate() (persistor.CachePersistor, error)
}

type CachePersistorConfiguration struct {
	Type string `json:"type"`
}

type FilesystemPersistorConfiguration struct {
	Root string `json:"root"`
	CachePersistorConfiguration
}

func (c *FilesystemPersistorConfiguration) Instantiate() (persistor.CachePersistor, error) {
	return filesystem.New(c.Root)
}

type InMemoryPersistorConfiguration struct {
	CachePersistorConfiguration
}

func (c *InMemoryPersistorConfiguration) Instantiate() (persistor.CachePersistor, error) {
	return inmemory.New()
}

func CreateCachePersistorInstantiatorFromJson(b []byte) (CachePersistorInstantiator, error) {
	var cpc CachePersistorConfiguration
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
