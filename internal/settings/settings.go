package settings

const defaultDomain = "localhost"
const defaultBindAddress = "0.0.0.0"
const defaultPort = "9000"
const defaultStoragePath = "./data"
const defaultUseFilesystemBlobStore = false

type Settings struct {
	domain                 *string
	bindAddress            *string
	port                   *string
	storagePath            *string
	useFilesystemBlobStore *bool
}

func valueOrDefault[V any](v *V, defaultValue V) V {
	if v == nil {
		return defaultValue
	}
	return *v
}

// TODO: use reflection to implement this
// otherwise we need to change the implementation
// everytime a new field is added to the Settings struct
func (s *Settings) merge(other *Settings) {
	if other.domain != nil {
		s.domain = other.domain
	}
	if other.bindAddress != nil {
		s.bindAddress = other.bindAddress
	}
	if other.port != nil {
		s.port = other.port
	}
	if other.storagePath != nil {
		s.storagePath = other.storagePath
	}
	if other.useFilesystemBlobStore != nil {
		s.useFilesystemBlobStore = other.useFilesystemBlobStore
	}

}

func (s *Settings) Domain() string {
	return valueOrDefault(s.domain, defaultDomain)
}

func (s *Settings) BindAddress() string {
	return valueOrDefault(s.bindAddress, defaultBindAddress)
}

func (s *Settings) Port() string {
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
