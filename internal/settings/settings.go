package settings

import (
	"log/slog"
	"reflect"
	"strings"
	"unsafe"
)

const defaultAuthenticationEnabled = true
const defaultRegion = "eu-central-1"
const defaultDomain = "localhost"
const defaultBindAddress = "0.0.0.0"
const defaultPort = 9000
const defaultHTTPEnabled = true
const defaultHTTPSEnabled = false
const defaultHTTPSPort = 9443
const defaultMonitoringPort = 9090
const defaultMonitoringPortEnabled = true
const defaultMonitoringHTTPSEnabled = false
const defaultMonitoringHTTPSPort = 9444
const defaultACMEEnabled = false
const defaultACMECADirectoryURL = "https://acme-v02.api.letsencrypt.org/directory"
const defaultACMEChallenge = "auto"
const defaultStorageJsonPath = "./storage.json"
const defaultAuthorizerPath = "./authorizer.lua"
const defaultSpoolDir = ""
const defaultTrustForwardedHeaders = false
const defaultOtelEnabled = false
const defaultWebsiteDomain = "s3-website.localhost"
const defaultOtelExporter = "otlp"
const defaultOtelEndpoint = "localhost:4318"

const mergableTagKey = "mergable"

type Credentials struct {
	AccessKeyId     string
	SecretAccessKey string
}

type Settings struct {
	authenticationEnabled  *bool         `mergable:""`
	credentials            []Credentials `mergable:""`
	region                 *string       `mergable:""`
	domain                 *string       `mergable:""`
	websiteDomain          *string       `mergable:""`
	bindAddress            *string       `mergable:""`
	port                   *int          `mergable:""`
	httpEnabled            *bool         `mergable:""`
	httpsEnabled           *bool         `mergable:""`
	httpsPort              *int          `mergable:""`
	monitoringPort         *int          `mergable:""`
	monitoringPortEnabled  *bool         `mergable:""`
	monitoringHttpsEnabled *bool         `mergable:""`
	monitoringHttpsPort    *int          `mergable:""`
	tlsCertFile            *string       `mergable:""`
	tlsKeyFile             *string       `mergable:""`
	acmeEnabled            *bool         `mergable:""`
	acmeDomains            []string      `mergable:""`
	acmeEmail              *string       `mergable:""`
	acmeCacheDir           *string       `mergable:""`
	acmeCADirectoryURL     *string       `mergable:""`
	acmeChallenge          *string       `mergable:""`
	acmeDNSProvider        *string       `mergable:""`
	storageJsonPath        *string       `mergable:""`
	authorizerPath         *string       `mergable:""`
	spoolDir               *string       `mergable:""`
	trustForwardedHeaders  *bool         `mergable:""`
	trustedProxyCIDRs      []string      `mergable:""`
	logLevel               *string       `mergable:""`
	otelEnabled            *bool         `mergable:""`
	otelExporter           *string       `mergable:""`
	otelEndpoint           *string       `mergable:""`
}

func valueOrDefault[V any](v *V, defaultValue V) V {
	if v == nil {
		return defaultValue
	}
	return *v
}

func (s *Settings) isAuthenticationEnabled() bool {
	return valueOrDefault(s.authenticationEnabled, defaultAuthenticationEnabled)
}

func (s *Settings) Credentials() []Credentials {
	if !s.isAuthenticationEnabled() {
		return nil
	}
	if s.credentials == nil {
		return []Credentials{}
	}
	return s.credentials
}

func (s *Settings) Region() string {
	return valueOrDefault(s.region, defaultRegion)
}

func (s *Settings) Domain() string {
	return valueOrDefault(s.domain, defaultDomain)
}

func (s *Settings) WebsiteDomain() string {
	return valueOrDefault(s.websiteDomain, defaultWebsiteDomain)
}

func (s *Settings) BindAddress() string {
	return valueOrDefault(s.bindAddress, defaultBindAddress)
}

func (s *Settings) Port() int {
	return valueOrDefault(s.port, defaultPort)
}

func (s *Settings) HTTPEnabled() bool {
	return valueOrDefault(s.httpEnabled, defaultHTTPEnabled)
}

func (s *Settings) HTTPSEnabled() bool {
	return valueOrDefault(s.httpsEnabled, defaultHTTPSEnabled)
}

func (s *Settings) HTTPSPort() int {
	return valueOrDefault(s.httpsPort, defaultHTTPSPort)
}

func (s *Settings) MonitoringPort() int {
	return valueOrDefault(s.monitoringPort, defaultMonitoringPort)
}

func (s *Settings) MonitoringPortEnabled() bool {
	return valueOrDefault(s.monitoringPortEnabled, defaultMonitoringPortEnabled)
}

func (s *Settings) MonitoringHTTPSEnabled() bool {
	return valueOrDefault(s.monitoringHttpsEnabled, defaultMonitoringHTTPSEnabled)
}

func (s *Settings) MonitoringHTTPSPort() int {
	return valueOrDefault(s.monitoringHttpsPort, defaultMonitoringHTTPSPort)
}

func (s *Settings) TLSCertFile() string {
	return valueOrDefault(s.tlsCertFile, "")
}

func (s *Settings) TLSKeyFile() string {
	return valueOrDefault(s.tlsKeyFile, "")
}

func (s *Settings) ACMEEnabled() bool {
	return valueOrDefault(s.acmeEnabled, defaultACMEEnabled)
}

func (s *Settings) ACMEDomains() []string {
	if s.acmeDomains == nil {
		return []string{}
	}
	return s.acmeDomains
}

func (s *Settings) ACMEEmail() string {
	return valueOrDefault(s.acmeEmail, "")
}

func (s *Settings) ACMECacheDir() string {
	return valueOrDefault(s.acmeCacheDir, "")
}

func (s *Settings) ACMECADirectoryURL() string {
	return valueOrDefault(s.acmeCADirectoryURL, defaultACMECADirectoryURL)
}

func (s *Settings) ACMEChallenge() string {
	return strings.ToLower(strings.TrimSpace(valueOrDefault(s.acmeChallenge, defaultACMEChallenge)))
}

func (s *Settings) ACMEDNSProvider() string {
	return strings.ToLower(strings.TrimSpace(valueOrDefault(s.acmeDNSProvider, "")))
}

func (s *Settings) StorageJsonPath() string {
	return valueOrDefault(s.storageJsonPath, defaultStorageJsonPath)
}

func (s *Settings) AuthorizerPath() string {
	return valueOrDefault(s.authorizerPath, defaultAuthorizerPath)
}

func (s *Settings) SpoolDir() string {
	return valueOrDefault(s.spoolDir, defaultSpoolDir)
}

func (s *Settings) TrustForwardedHeaders() bool {
	return valueOrDefault(s.trustForwardedHeaders, defaultTrustForwardedHeaders)
}

func (s *Settings) TrustedProxyCIDRs() []string {
	if s.trustedProxyCIDRs == nil {
		return []string{}
	}
	return s.trustedProxyCIDRs
}

func (s *Settings) LogLevel() slog.Level {
	logLevel := valueOrDefault(s.logLevel, slog.LevelInfo.String())
	switch strings.ToUpper(logLevel) {
	case slog.LevelDebug.String():
		return slog.LevelDebug
	case slog.LevelInfo.String():
		return slog.LevelInfo
	case slog.LevelWarn.String():
		return slog.LevelWarn
	case slog.LevelError.String():
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func (s *Settings) OtelEnabled() bool {
	return valueOrDefault(s.otelEnabled, defaultOtelEnabled)
}

func (s *Settings) OtelExporter() string {
	return valueOrDefault(s.otelExporter, defaultOtelExporter)
}

func (s *Settings) OtelEndpoint() string {
	return valueOrDefault(s.otelEndpoint, defaultOtelEndpoint)
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

		if field.Type.Kind() == reflect.Pointer || field.Type.Kind() == reflect.Slice {
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
