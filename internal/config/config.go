package config

import "github.com/jdillenkofer/pithos/internal/dependencyinjection"

type DynamicJsonType struct {
	Type string `json:"type"`
}

type DynamicJsonInstantiator[T any] interface {
	Instantiate(diProvider dependencyinjection.DIProvider) (T, error)
}
