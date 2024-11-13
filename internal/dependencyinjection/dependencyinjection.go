package dependencyinjection

import (
	"errors"
	"reflect"
)

var ErrNoSuchType = errors.New("no such registered type")
var ErrTypeAlreadyRegistered = errors.New("type already registered")
var ErrNoSuchName = errors.New("no such registered name")
var ErrNameAlreadyRegistered = errors.New("name already registered")

type DIProvider interface {
	LookupByType(t reflect.Type) (interface{}, error)
	LookupByName(name string) (interface{}, error)
}

type DICollection interface {
	RegisterSingletonByType(t reflect.Type, obj interface{}) error
	RegisterSingletonByName(name string, obj interface{}) error
}

type DIContainer interface {
	DICollection
	DIProvider
}

func NewContainer() (DIContainer, error) {
	return &diContainer{
		mapOfTypeToObject: make(map[reflect.Type]interface{}),
		mapOfNameToObject: make(map[string]interface{}),
	}, nil
}

type diContainer struct {
	mapOfTypeToObject map[reflect.Type]interface{}
	mapOfNameToObject map[string]interface{}
}

func (diContainer *diContainer) LookupByType(t reflect.Type) (interface{}, error) {
	obj, ok := diContainer.mapOfTypeToObject[t]
	if !ok {
		return nil, ErrNoSuchType
	}
	return obj, nil
}

func (diContainer *diContainer) RegisterSingletonByType(t reflect.Type, obj interface{}) error {
	_, ok := diContainer.mapOfTypeToObject[t]
	if ok {
		return ErrTypeAlreadyRegistered
	}
	diContainer.mapOfTypeToObject[t] = obj
	return nil
}

func (diContainer *diContainer) LookupByName(name string) (interface{}, error) {
	obj, ok := diContainer.mapOfNameToObject[name]
	if !ok {
		return nil, ErrNoSuchName
	}
	return obj, nil
}

func (diContainer *diContainer) RegisterSingletonByName(name string, obj interface{}) error {
	_, ok := diContainer.mapOfNameToObject[name]
	if ok {
		return ErrNameAlreadyRegistered
	}
	diContainer.mapOfNameToObject[name] = obj
	return nil
}
