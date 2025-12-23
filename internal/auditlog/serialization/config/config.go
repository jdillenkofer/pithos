package config

import (
	"encoding/json"
	"errors"

	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
)

const (
	binarySerializerType = "BinarySerializer"
	jsonSerializerType   = "JsonSerializer"
	textSerializerType   = "TextSerializer"
)

type SerializerInstantiator = internalConfig.DynamicJsonInstantiator[serialization.Serializer]

type BinarySerializerConfiguration struct {
	internalConfig.DynamicJsonType
}

func (b *BinarySerializerConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (b *BinarySerializerConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (serialization.Serializer, error) {
	return &serialization.BinarySerializer{}, nil
}

type JsonSerializerConfiguration struct {
	Indent bool `json:"indent"`
	internalConfig.DynamicJsonType
}

func (j *JsonSerializerConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (j *JsonSerializerConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (serialization.Serializer, error) {
	return &serialization.JsonSerializer{Indent: j.Indent}, nil
}

type TextSerializerConfiguration struct {
	internalConfig.DynamicJsonType
}

func (t *TextSerializerConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (t *TextSerializerConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (serialization.Serializer, error) {
	return &serialization.TextSerializer{}, nil
}

func CreateSerializerInstantiatorFromJson(b []byte) (SerializerInstantiator, error) {
	var sc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &sc)
	if err != nil {
		return nil, err
	}

	var si SerializerInstantiator
	switch sc.Type {
	case binarySerializerType:
		si = &BinarySerializerConfiguration{}
	case jsonSerializerType:
		si = &JsonSerializerConfiguration{}
	case textSerializerType:
		si = &TextSerializerConfiguration{}
	default:
		return nil, errors.New("unknown serializer type")
	}
	err = json.Unmarshal(b, &si)
	if err != nil {
		return nil, err
	}
	return si, nil
}
