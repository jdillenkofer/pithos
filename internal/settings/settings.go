package settings

import (
	"reflect"
	"unsafe"
)

const defaultRegion = "eu-central-1"
const defaultDomain = "localhost"
const defaultBindAddress = "0.0.0.0"
const defaultPort = 9000
const defaultMetricPort = 9001
const defaultStoragePath = "./data"
const defaultUseFilesystemBlobStore = false
const defaultWrapBlobStoreWithOutbox = false
const defaultReplicationUseOutbox = true

const mergableTagKey = "mergable"

type ReplicationSettings struct {
	accessKeyId     *string
	secretAccessKey *string
	region          *string
	endpoint        *string
	useOutbox       *bool
}

type Settings struct {
	accessKeyId             *string              `mergable:""`
	secretAccessKey         *string              `mergable:""`
	region                  *string              `mergable:""`
	domain                  *string              `mergable:""`
	bindAddress             *string              `mergable:""`
	port                    *int                 `mergable:""`
	metricPort              *int                 `mergable:""`
	storagePath             *string              `mergable:""`
	useFilesystemBlobStore  *bool                `mergable:""`
	wrapBlobStoreWithOutbox *bool                `mergable:""`
	replication             *ReplicationSettings `mergable:""`
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

func (s *Settings) MetricPort() int {
	return valueOrDefault(s.metricPort, defaultMetricPort)
}

func (s *Settings) StoragePath() string {
	return valueOrDefault(s.storagePath, defaultStoragePath)
}

func (s *Settings) UseFilesystemBlobStore() bool {
	return valueOrDefault(s.useFilesystemBlobStore, defaultUseFilesystemBlobStore)
}

func (s *Settings) WrapBlobStoreWithOutbox() bool {
	defaultWrapBlobStoreWithOutbox := s.UseFilesystemBlobStore()
	return valueOrDefault(s.wrapBlobStoreWithOutbox, defaultWrapBlobStoreWithOutbox)
}

func (s *Settings) Replication() *ReplicationSettings {
	return s.replication
}

func (r *ReplicationSettings) AccessKeyId() string {
	return valueOrDefault(r.accessKeyId, "")
}

func (r *ReplicationSettings) SecretAccessKey() string {
	return valueOrDefault(r.secretAccessKey, "")
}

func (r *ReplicationSettings) Region() string {
	return valueOrDefault(r.region, defaultRegion)
}

func (r *ReplicationSettings) Endpoint() *string {
	return r.endpoint
}

func (r *ReplicationSettings) UseOutbox() bool {
	return valueOrDefault(r.useOutbox, true)
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

func LoadSettings() (*Settings, error) {
	jsonSettings, _ := loadSettingsFromJson("config.json")
	cmdArgsSettings, _ := loadSettingsFromCmdArgs()
	envSettings, _ := loadSettingsFromEnv()
	settings := mergeSettings(jsonSettings, cmdArgsSettings, envSettings)
	return settings, nil
}
