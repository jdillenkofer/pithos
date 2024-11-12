package dependencyinjection

import (
	"errors"
	"reflect"
)

var ErrNoSuchType = errors.New("no such registered type")

type DIContainer interface {
	LookupByType(t reflect.Type) (interface{}, error)
	LookupByName(name string) (interface{}, error)
	RegisterSingletonByType(t reflect.Type, obj interface{})
	RegisterSingletonByName(name string, obj interface{})
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

func (diContainer *diContainer) RegisterSingletonByType(t reflect.Type, obj interface{}) {
	diContainer.mapOfTypeToObject[t] = obj
}

func (diContainer *diContainer) LookupByName(name string) (interface{}, error) {
	obj, ok := diContainer.mapOfNameToObject[name]
	if !ok {
		return nil, ErrNoSuchType
	}
	return obj, nil
}

func (diContainer *diContainer) RegisterSingletonByName(name string, obj interface{}) {
	diContainer.mapOfNameToObject[name] = obj
}
