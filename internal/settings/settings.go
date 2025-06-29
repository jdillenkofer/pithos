package settings

import (
	"reflect"
	"unsafe"
)

const defaultRegion = "eu-central-1"
const defaultDomain = "localhost"
const defaultBindAddress = "0.0.0.0"
const defaultPort = 9000
const defaultMonitoringPort = 9090
const defaultMonitoringPortEnabled = true
const defaultStorageJsonPath = "./storage.json"
const defaultAuthorizerPath = "./authorizer.lua"

const mergableTagKey = "mergable"

type Settings struct {
	accessKeyId           *string `mergable:""`
	secretAccessKey       *string `mergable:""`
	region                *string `mergable:""`
	domain                *string `mergable:""`
	bindAddress           *string `mergable:""`
	port                  *int    `mergable:""`
	monitoringPort        *int    `mergable:""`
	monitoringPortEnabled *bool   `mergable:""`
	storageJsonPath       *string `mergable:""`
	authorizerPath        *string `mergable:""`
}

func valueOrDefault[V any](v *V, defaultValue V) V {
	if v == nil {
		return defaultValue
	}
	return *v
}

func (s *Settings) AccessKeyId() string {
	return valueOrDefault(s.accessKeyId, "")
}

func (s *Settings) SecretAccessKey() string {
	return valueOrDefault(s.secretAccessKey, "")
}

func (s *Settings) Region() string {
	return valueOrDefault(s.region, defaultRegion)
}

func (s *Settings) Domain() string {
	return valueOrDefault(s.domain, defaultDomain)
}

func (s *Settings) BindAddress() string {
	return valueOrDefault(s.bindAddress, defaultBindAddress)
}

func (s *Settings) Port() int {
	return valueOrDefault(s.port, defaultPort)
}

func (s *Settings) MonitoringPort() int {
	return valueOrDefault(s.monitoringPort, defaultMonitoringPort)
}

func (s *Settings) MonitoringPortEnabled() bool {
	return valueOrDefault(s.monitoringPortEnabled, defaultMonitoringPortEnabled)
}

func (s *Settings) StorageJsonPath() string {
	return valueOrDefault(s.storageJsonPath, defaultStorageJsonPath)
}

func (s *Settings) AuthorizerPath() string {
	return valueOrDefault(s.authorizerPath, defaultAuthorizerPath)
}

func getUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

func setUnexportedField(field reflect.Value, value interface{}) {
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

func isNilish(val any) bool {
	if val == nil {
		return true
	}

	v := reflect.ValueOf(val)
	k := v.Kind()
	switch k {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer,
		reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return v.IsNil()
	}

	return false
}

func (s *Settings) merge(other *Settings) {
	fields := reflect.VisibleFields(reflect.TypeOf(other).Elem())
	sStruct := reflect.ValueOf(s).Elem()
	otherStruct := reflect.ValueOf(other).Elem()

	for _, field := range fields {
		if _, ok := field.Tag.Lookup(mergableTagKey); !ok {
			continue
		}
		sField := sStruct.FieldByName(field.Name)
		otherField := otherStruct.FieldByName(field.Name)

		if field.Type.Kind() == reflect.Pointer {
			otherFieldValue := getUnexportedField(otherField)
			if !isNilish(otherFieldValue) {
				setUnexportedField(sField, otherFieldValue)
			}
		} else {
			otherFieldValue := getUnexportedField(otherField)
			setUnexportedField(sField, otherFieldValue)

		}
	}
}

func mergeSettings(settings ...*Settings) *Settings {
	var result *Settings = &Settings{}
	for _, setting := range settings {
		if setting == nil {
			continue
		}
		result.merge(setting)
	}
	return result
}

func LoadSettings(cmdArgs []string) (*Settings, error) {
	cmdArgsSettings, err := loadSettingsFromCmdArgs(cmdArgs)
	if err != nil {
		return nil, err
	}
	envSettings, err := loadSettingsFromEnv()
	if err != nil {
		return nil, err
	}
	settings := mergeSettings(cmdArgsSettings, envSettings)
	return settings, nil
}
