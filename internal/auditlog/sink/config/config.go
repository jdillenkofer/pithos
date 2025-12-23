package config

import (
	"encoding/json"
	"errors"

	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
	serializationConfig "github.com/jdillenkofer/pithos/internal/auditlog/serialization/config"
	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
)

const (
	fileSinkType = "FileSink"
)

type SinkInstantiator = internalConfig.DynamicJsonInstantiator[sink.Sink]

type FileSinkConfiguration struct {
	Path                   string                                     `json:"path"`
	SerializerInstantiator serializationConfig.SerializerInstantiator `json:"-"`
	RawSerializer          json.RawMessage                            `json:"serializer"`
	internalConfig.DynamicJsonType
}

func (f *FileSinkConfiguration) UnmarshalJSON(b []byte) error {
	type fileSinkConfiguration FileSinkConfiguration
	err := json.Unmarshal(b, (*fileSinkConfiguration)(f))
	if err != nil {
		return err
	}

	f.SerializerInstantiator, err = serializationConfig.CreateSerializerInstantiatorFromJson(f.RawSerializer)
	if err != nil {
		return err
	}
	return nil
}

func (f *FileSinkConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return f.SerializerInstantiator.RegisterReferences(diCollection)
}

func (f *FileSinkConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (sink.Sink, error) {
	serializer, err := f.SerializerInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return sink.NewFileSink(f.Path, serializer)
}

func CreateSinkInstantiatorFromJson(b []byte) (SinkInstantiator, error) {
	var sc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &sc)
	if err != nil {
		return nil, err
	}

	var si SinkInstantiator
	switch sc.Type {
	case fileSinkType:
		si = &FileSinkConfiguration{}
	default:
		return nil, errors.New("unknown sink type")
	}
	err = json.Unmarshal(b, &si)
	if err != nil {
		return nil, err
	}
	return si, nil
}
