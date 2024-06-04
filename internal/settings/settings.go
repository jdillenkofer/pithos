package settings

import (
	"reflect"
	"unsafe"
)

const defaultDomain = "localhost"
const defaultBindAddress = "0.0.0.0"
const defaultPort = 9000
const defaultStoragePath = "./data"
const defaultUseFilesystemBlobStore = false

type Settings struct {
	domain                 *string
	bindAddress            *string
	port                   *int
	storagePath            *string
	useFilesystemBlobStore *bool
}

func valueOrDefault[V any](v *V, defaultValue V) V {
	if v == nil {
		return defaultValue
	}
	return *v
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

func (s *Settings) Domain() string {
	return valueOrDefault(s.domain, defaultDomain)
}

func (s *Settings) BindAddress() string {
	return valueOrDefault(s.bindAddress, defaultBindAddress)
}

func (s *Settings) Port() int {
	return valueOrDefault(s.port, defaultPort)
}

func (s *Settings) StoragePath() string {
	return valueOrDefault(s.storagePath, defaultStoragePath)
}

func (s *Settings) UseFilesystemBlobStore() bool {
	return valueOrDefault(s.useFilesystemBlobStore, defaultUseFilesystemBlobStore)
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

func LoadSettings() (*Settings, error) {
	jsonSettings, _ := loadSettingsFromJson("config.json")
	cmdArgsSettings, _ := loadSettingsFromCmdArgs()
	envSettings, _ := loadSettingsFromEnv()
	settings := mergeSettings(jsonSettings, cmdArgsSettings, envSettings)
	return settings, nil
}
