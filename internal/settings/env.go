package settings

import (
	"os"
	"strconv"
	"strings"
)

const envKeyPrefix = "PITHOS"

const authenticationEnabledEnvKey = envKeyPrefix + "_AUTHENTICATION_ENABLED"
const regionEnvKey = envKeyPrefix + "_REGION"
const domainEnvKey = envKeyPrefix + "_DOMAIN"
const websiteDomainEnvKey = envKeyPrefix + "_WEBSITE_DOMAIN"
const bindAddressEnvKey = envKeyPrefix + "_BIND_ADDRESS"
const portEnvKey = envKeyPrefix + "_PORT"
const httpEnabledEnvKey = envKeyPrefix + "_HTTP_ENABLED"
const httpsEnabledEnvKey = envKeyPrefix + "_HTTPS_ENABLED"
const httpsPortEnvKey = envKeyPrefix + "_HTTPS_PORT"
const monitoringPortEnvKey = envKeyPrefix + "_MONITORING_PORT"
const monitoringPortEnabledEnvKey = envKeyPrefix + "_MONITORING_PORT_ENABLED"
const monitoringHttpsEnabledEnvKey = envKeyPrefix + "_MONITORING_HTTPS_ENABLED"
const monitoringHttpsPortEnvKey = envKeyPrefix + "_MONITORING_HTTPS_PORT"
const tlsCertFileEnvKey = envKeyPrefix + "_TLS_CERT_FILE"
const tlsKeyFileEnvKey = envKeyPrefix + "_TLS_KEY_FILE"
const acmeEnabledEnvKey = envKeyPrefix + "_ACME_ENABLED"
const acmeDomainsEnvKey = envKeyPrefix + "_ACME_DOMAINS"
const acmeEmailEnvKey = envKeyPrefix + "_ACME_EMAIL"
const acmeCacheDirEnvKey = envKeyPrefix + "_ACME_CACHE_DIR"
const acmeCADirectoryURLEnvKey = envKeyPrefix + "_ACME_CA_DIRECTORY_URL"
const acmeChallengeEnvKey = envKeyPrefix + "_ACME_CHALLENGE"
const acmeDNSProviderEnvKey = envKeyPrefix + "_ACME_DNS_PROVIDER"
const storageJsonPathEnvKey = envKeyPrefix + "_STORAGE_JSON_PATH"
const authorizerPathEnvKey = envKeyPrefix + "_AUTHORIZER_PATH"
const spoolDirEnvKey = envKeyPrefix + "_SPOOL_DIR"
const trustForwardedHeadersEnvKey = envKeyPrefix + "_TRUST_FORWARDED_HEADERS"
const trustedProxyCIDRsEnvKey = envKeyPrefix + "_TRUSTED_PROXY_CIDRS"
const logLevelEnvKey = envKeyPrefix + "_LOG_LEVEL"
const otelEnabledEnvKey = envKeyPrefix + "_OTEL_ENABLED"
const otelExporterEnvKey = envKeyPrefix + "_OTEL_EXPORTER"
const otelEndpointEnvKey = envKeyPrefix + "_OTEL_ENDPOINT"

func getCredentialsFromEnv() []Credentials {
	var credentials []Credentials = nil
	for i := 0; ; i++ {
		accessKeyId := getStringFromEnv(envKeyPrefix + "_CREDENTIALS_" + strconv.Itoa(i) + "_ACCESS_KEY_ID")
		secretAccessKey := getStringFromEnv(envKeyPrefix + "_CREDENTIALS_" + strconv.Itoa(i) + "_SECRET_ACCESS_KEY")

		if accessKeyId == nil || secretAccessKey == nil {
			// This allows the index to start from 0 or 1
			if i == 0 {
				continue
			}
			break
		}

		credentials = append(credentials, Credentials{
			AccessKeyId:     *accessKeyId,
			SecretAccessKey: *secretAccessKey,
		})
	}

	return credentials
}

func getStringFromEnv(envKey string) *string {
	val := os.Getenv(envKey)
	if val == "" {
		return nil
	}
	return &val
}

func getIntFromEnv(envKey string) *int {
	val := os.Getenv(envKey)
	if val == "" {
		return nil
	}
	int64Val, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return nil
	}
	intVal := int(int64Val)
	return &intVal
}

func getBoolFromEnv(envKey string) *bool {
	val := os.Getenv(envKey)
	val = strings.ToLower(val)
	if val == "" {
		return nil
	}
	retval := val == "1" || val == "t" || val == "true"
	return &retval
}

func getStringSliceFromEnv(envKey string) []string {
	val := os.Getenv(envKey)
	if val == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func splitCommaSeparated(val string) []string {
	parts := strings.Split(val, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func loadSettingsFromEnv() (*Settings, error) {
	credentials := getCredentialsFromEnv()
	authenticationEnabled := getBoolFromEnv(authenticationEnabledEnvKey)
	region := getStringFromEnv(regionEnvKey)
	domain := getStringFromEnv(domainEnvKey)
	websiteDomain := getStringFromEnv(websiteDomainEnvKey)
	bindAddress := getStringFromEnv(bindAddressEnvKey)
	port := getIntFromEnv(portEnvKey)
	httpEnabled := getBoolFromEnv(httpEnabledEnvKey)
	httpsEnabled := getBoolFromEnv(httpsEnabledEnvKey)
	httpsPort := getIntFromEnv(httpsPortEnvKey)
	monitoringPort := getIntFromEnv(monitoringPortEnvKey)
	monitoringPortEnabled := getBoolFromEnv(monitoringPortEnabledEnvKey)
	monitoringHttpsEnabled := getBoolFromEnv(monitoringHttpsEnabledEnvKey)
	monitoringHttpsPort := getIntFromEnv(monitoringHttpsPortEnvKey)
	tlsCertFile := getStringFromEnv(tlsCertFileEnvKey)
	tlsKeyFile := getStringFromEnv(tlsKeyFileEnvKey)
	acmeEnabled := getBoolFromEnv(acmeEnabledEnvKey)
	acmeDomains := getStringSliceFromEnv(acmeDomainsEnvKey)
	acmeEmail := getStringFromEnv(acmeEmailEnvKey)
	acmeCacheDir := getStringFromEnv(acmeCacheDirEnvKey)
	acmeCADirectoryURL := getStringFromEnv(acmeCADirectoryURLEnvKey)
	acmeChallenge := getStringFromEnv(acmeChallengeEnvKey)
	acmeDNSProvider := getStringFromEnv(acmeDNSProviderEnvKey)
	storageJsonPath := getStringFromEnv(storageJsonPathEnvKey)
	authorizerPath := getStringFromEnv(authorizerPathEnvKey)
	spoolDir := getStringFromEnv(spoolDirEnvKey)
	trustForwardedHeaders := getBoolFromEnv(trustForwardedHeadersEnvKey)
	trustedProxyCIDRs := getStringSliceFromEnv(trustedProxyCIDRsEnvKey)
	logLevel := getStringFromEnv(logLevelEnvKey)
	otelEnabled := getBoolFromEnv(otelEnabledEnvKey)
	otelExporter := getStringFromEnv(otelExporterEnvKey)
	otelEndpoint := getStringFromEnv(otelEndpointEnvKey)

	return &Settings{
		authenticationEnabled:  authenticationEnabled,
		credentials:            credentials,
		region:                 region,
		domain:                 domain,
		websiteDomain:          websiteDomain,
		bindAddress:            bindAddress,
		port:                   port,
		httpEnabled:            httpEnabled,
		httpsEnabled:           httpsEnabled,
		httpsPort:              httpsPort,
		monitoringPort:         monitoringPort,
		monitoringPortEnabled:  monitoringPortEnabled,
		monitoringHttpsEnabled: monitoringHttpsEnabled,
		monitoringHttpsPort:    monitoringHttpsPort,
		tlsCertFile:            tlsCertFile,
		tlsKeyFile:             tlsKeyFile,
		acmeEnabled:            acmeEnabled,
		acmeDomains:            acmeDomains,
		acmeEmail:              acmeEmail,
		acmeCacheDir:           acmeCacheDir,
		acmeCADirectoryURL:     acmeCADirectoryURL,
		acmeChallenge:          acmeChallenge,
		acmeDNSProvider:        acmeDNSProvider,
		storageJsonPath:        storageJsonPath,
		authorizerPath:         authorizerPath,
		spoolDir:               spoolDir,
		trustForwardedHeaders:  trustForwardedHeaders,
		trustedProxyCIDRs:      trustedProxyCIDRs,
		logLevel:               logLevel,
		otelEnabled:            otelEnabled,
		otelExporter:           otelExporter,
		otelEndpoint:           otelEndpoint,
	}, nil
}
