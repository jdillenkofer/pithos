package config

import "github.com/jdillenkofer/pithos/internal/dependencyinjection"

type DynamicJsonType struct {
	Type string `json:"type"`
}

type DynamicJsonInstantiator[T any] interface {
	Instantiate(diContainer dependencyinjection.DIContainer) (T, error)
}
