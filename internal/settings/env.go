package settings

import (
	"os"
	"strconv"
	"strings"
)

const envKeyPrefix = "PITHOS"

const authenticationEnabledEnvKey = envKeyPrefix + "_AUTHENTICATION_ENABLED"
const credentialsProviderEnvKey = envKeyPrefix + "_CREDENTIALS_PROVIDER"
const credentialsPathEnvKey = envKeyPrefix + "_CREDENTIALS_PATH"
const credentialsReloadIntervalSecondsEnvKey = envKeyPrefix + "_CREDENTIALS_RELOAD_INTERVAL_SECONDS"
const credentialsDatabaseIndexEnvKey = envKeyPrefix + "_CREDENTIALS_DATABASE_INDEX"
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
const authorizerTypeEnvKey = envKeyPrefix + "_AUTHORIZER_TYPE"
const authorizerTimeoutMillisecondsEnvKey = envKeyPrefix + "_AUTHORIZER_TIMEOUT_MILLISECONDS"
const policyPathEnvKey = envKeyPrefix + "_POLICY_PATH"
const policyReloadIntervalSecondsEnvKey = envKeyPrefix + "_POLICY_RELOAD_INTERVAL_SECONDS"
const spoolDirEnvKey = envKeyPrefix + "_SPOOL_DIR"
const trustForwardedHeadersEnvKey = envKeyPrefix + "_TRUST_FORWARDED_HEADERS"
const trustedProxyCIDRsEnvKey = envKeyPrefix + "_TRUSTED_PROXY_CIDRS"
const logLevelEnvKey = envKeyPrefix + "_LOG_LEVEL"
const otelEnabledEnvKey = envKeyPrefix + "_OTEL_ENABLED"
const otelExporterEnvKey = envKeyPrefix + "_OTEL_EXPORTER"
const otelEndpointEnvKey = envKeyPrefix + "_OTEL_ENDPOINT"
const metricsGaugesIntervalSecondsEnvKey = envKeyPrefix + "_METRICS_GAUGES_INTERVAL_SECONDS"

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
	authenticationEnabled := getBoolFromEnv(authenticationEnabledEnvKey)
	credentialsProvider := getStringFromEnv(credentialsProviderEnvKey)
	credentialsPath := getStringFromEnv(credentialsPathEnvKey)
	credentialsReloadIntervalSeconds := getIntFromEnv(credentialsReloadIntervalSecondsEnvKey)
	credentialsDatabaseIndex := getIntFromEnv(credentialsDatabaseIndexEnvKey)
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
	authorizerType := getStringFromEnv(authorizerTypeEnvKey)
	authorizerTimeoutMilliseconds := getIntFromEnv(authorizerTimeoutMillisecondsEnvKey)
	policyPath := getStringFromEnv(policyPathEnvKey)
	policyReloadIntervalSeconds := getIntFromEnv(policyReloadIntervalSecondsEnvKey)
	spoolDir := getStringFromEnv(spoolDirEnvKey)
	trustForwardedHeaders := getBoolFromEnv(trustForwardedHeadersEnvKey)
	trustedProxyCIDRs := getStringSliceFromEnv(trustedProxyCIDRsEnvKey)
	logLevel := getStringFromEnv(logLevelEnvKey)
	otelEnabled := getBoolFromEnv(otelEnabledEnvKey)
	otelExporter := getStringFromEnv(otelExporterEnvKey)
	otelEndpoint := getStringFromEnv(otelEndpointEnvKey)
	metricsGaugesIntervalSeconds := getIntFromEnv(metricsGaugesIntervalSecondsEnvKey)

	return &Settings{
		authenticationEnabled:            authenticationEnabled,
		credentialsProvider:              credentialsProvider,
		credentialsPath:                  credentialsPath,
		credentialsReloadIntervalSeconds: credentialsReloadIntervalSeconds,
		credentialsDatabaseIndex:         credentialsDatabaseIndex,
		region:                           region,
		domain:                           domain,
		websiteDomain:                    websiteDomain,
		bindAddress:                      bindAddress,
		port:                             port,
		httpEnabled:                      httpEnabled,
		httpsEnabled:                     httpsEnabled,
		httpsPort:                        httpsPort,
		monitoringPort:                   monitoringPort,
		monitoringPortEnabled:            monitoringPortEnabled,
		monitoringHttpsEnabled:           monitoringHttpsEnabled,
		monitoringHttpsPort:              monitoringHttpsPort,
		tlsCertFile:                      tlsCertFile,
		tlsKeyFile:                       tlsKeyFile,
		acmeEnabled:                      acmeEnabled,
		acmeDomains:                      acmeDomains,
		acmeEmail:                        acmeEmail,
		acmeCacheDir:                     acmeCacheDir,
		acmeCADirectoryURL:               acmeCADirectoryURL,
		acmeChallenge:                    acmeChallenge,
		acmeDNSProvider:                  acmeDNSProvider,
		storageJsonPath:                  storageJsonPath,
		authorizerPath:                   authorizerPath,
		authorizerType:                   authorizerType,
		authorizerTimeoutMilliseconds:    authorizerTimeoutMilliseconds,
		policyPath:                       policyPath,
		policyReloadIntervalSeconds:      policyReloadIntervalSeconds,
		spoolDir:                         spoolDir,
		trustForwardedHeaders:            trustForwardedHeaders,
		trustedProxyCIDRs:                trustedProxyCIDRs,
		logLevel:                         logLevel,
		otelEnabled:                      otelEnabled,
		otelExporter:                     otelExporter,
		otelEndpoint:                     otelEndpoint,
		metricsGaugesIntervalSeconds:     metricsGaugesIntervalSeconds,
	}, nil
}
