package config

type DynamicJsonType struct {
	Type string `json:"type"`
}

type DynamicJsonInstantiator[T any] interface {
	Instantiate() (T, error)
}
