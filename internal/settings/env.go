package settings

import (
	"fmt"
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
const monitoringPortEnvKey = envKeyPrefix + "_MONITORING_PORT"
const monitoringPortEnabledEnvKey = envKeyPrefix + "_MONITORING_PORT_ENABLED"
const adminEnabledEnvKey = envKeyPrefix + "_ADMIN_ENABLED"
const adminPortEnvKey = envKeyPrefix + "_ADMIN_PORT"
const adminBindAddressEnvKey = envKeyPrefix + "_ADMIN_BIND_ADDRESS"
const adminSecureCookieEnvKey = envKeyPrefix + "_ADMIN_SECURE_COOKIE"
const adminSessionIdleMinutesEnvKey = envKeyPrefix + "_ADMIN_SESSION_IDLE_MINUTES"
const adminSessionMaxHoursEnvKey = envKeyPrefix + "_ADMIN_SESSION_MAX_HOURS"
const adminDatabaseIndexEnvKey = envKeyPrefix + "_ADMIN_DATABASE_INDEX"
const adminAccessKeysEnvKey = envKeyPrefix + "_ADMIN_ACCESS_KEYS"
const storageJsonPathEnvKey = envKeyPrefix + "_STORAGE_JSON_PATH"
const authorizerPathEnvKey = envKeyPrefix + "_AUTHORIZER_PATH"
const authorizerTypeEnvKey = envKeyPrefix + "_AUTHORIZER_TYPE"
const authorizerTimeoutMillisecondsEnvKey = envKeyPrefix + "_AUTHORIZER_TIMEOUT_MILLISECONDS"
const authorizerMaxInstructionsEnvKey = envKeyPrefix + "_AUTHORIZER_MAX_INSTRUCTIONS"
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

func getIntFromEnv(envKey string) (*int, error) {
	val := os.Getenv(envKey)
	if val == "" {
		return nil, nil
	}
	int64Val, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("%s must be a 32-bit decimal integer", envKey)
	}
	intVal := int(int64Val)
	return &intVal, nil
}

func getBoolFromEnv(envKey string) (*bool, error) {
	val := os.Getenv(envKey)
	if val == "" {
		return nil, nil
	}
	retval, err := strconv.ParseBool(strings.ToLower(val))
	if err != nil {
		return nil, fmt.Errorf("%s must be a boolean (true, false, t, f, 1, or 0)", envKey)
	}
	return &retval, nil
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

func loadSettingsFromEnv() (*Settings, error) {
	authenticationEnabled, err := getBoolFromEnv(authenticationEnabledEnvKey)
	if err != nil {
		return nil, err
	}
	credentialsProvider := getStringFromEnv(credentialsProviderEnvKey)
	credentialsPath := getStringFromEnv(credentialsPathEnvKey)
	credentialsReloadIntervalSeconds, err := getIntFromEnv(credentialsReloadIntervalSecondsEnvKey)
	if err != nil {
		return nil, err
	}
	credentialsDatabaseIndex, err := getIntFromEnv(credentialsDatabaseIndexEnvKey)
	if err != nil {
		return nil, err
	}
	region := getStringFromEnv(regionEnvKey)
	domain := getStringFromEnv(domainEnvKey)
	websiteDomain := getStringFromEnv(websiteDomainEnvKey)
	bindAddress := getStringFromEnv(bindAddressEnvKey)
	port, err := getIntFromEnv(portEnvKey)
	if err != nil {
		return nil, err
	}
	monitoringPort, err := getIntFromEnv(monitoringPortEnvKey)
	if err != nil {
		return nil, err
	}
	monitoringPortEnabled, err := getBoolFromEnv(monitoringPortEnabledEnvKey)
	if err != nil {
		return nil, err
	}
	adminEnabled, err := getBoolFromEnv(adminEnabledEnvKey)
	if err != nil {
		return nil, err
	}
	adminPort, err := getIntFromEnv(adminPortEnvKey)
	if err != nil {
		return nil, err
	}
	adminSecureCookie, err := getBoolFromEnv(adminSecureCookieEnvKey)
	if err != nil {
		return nil, err
	}
	adminSessionIdleMinutes, err := getIntFromEnv(adminSessionIdleMinutesEnvKey)
	if err != nil {
		return nil, err
	}
	adminSessionMaxHours, err := getIntFromEnv(adminSessionMaxHoursEnvKey)
	if err != nil {
		return nil, err
	}
	adminDatabaseIndex, err := getIntFromEnv(adminDatabaseIndexEnvKey)
	if err != nil {
		return nil, err
	}
	storageJsonPath := getStringFromEnv(storageJsonPathEnvKey)
	authorizerPath := getStringFromEnv(authorizerPathEnvKey)
	authorizerType := getStringFromEnv(authorizerTypeEnvKey)
	authorizerTimeoutMilliseconds, err := getIntFromEnv(authorizerTimeoutMillisecondsEnvKey)
	if err != nil {
		return nil, err
	}
	authorizerMaxInstructions, err := getIntFromEnv(authorizerMaxInstructionsEnvKey)
	if err != nil {
		return nil, err
	}
	policyPath := getStringFromEnv(policyPathEnvKey)
	policyReloadIntervalSeconds, err := getIntFromEnv(policyReloadIntervalSecondsEnvKey)
	if err != nil {
		return nil, err
	}
	spoolDir := getStringFromEnv(spoolDirEnvKey)
	trustForwardedHeaders, err := getBoolFromEnv(trustForwardedHeadersEnvKey)
	if err != nil {
		return nil, err
	}
	trustedProxyCIDRs := getStringSliceFromEnv(trustedProxyCIDRsEnvKey)
	logLevel := getStringFromEnv(logLevelEnvKey)
	otelEnabled, err := getBoolFromEnv(otelEnabledEnvKey)
	if err != nil {
		return nil, err
	}
	otelExporter := getStringFromEnv(otelExporterEnvKey)
	otelEndpoint := getStringFromEnv(otelEndpointEnvKey)
	metricsGaugesIntervalSeconds, err := getIntFromEnv(metricsGaugesIntervalSecondsEnvKey)
	if err != nil {
		return nil, err
	}

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
		monitoringPort:                   monitoringPort,
		monitoringPortEnabled:            monitoringPortEnabled,
		adminEnabled:                     adminEnabled,
		adminPort:                        adminPort,
		adminBindAddress:                 getStringFromEnv(adminBindAddressEnvKey),
		adminSecureCookie:                adminSecureCookie,
		adminSessionIdleMinutes:          adminSessionIdleMinutes,
		adminSessionMaxHours:             adminSessionMaxHours,
		adminDatabaseIndex:               adminDatabaseIndex,
		adminAccessKeys:                  getStringSliceFromEnv(adminAccessKeysEnvKey),
		storageJsonPath:                  storageJsonPath,
		authorizerPath:                   authorizerPath,
		authorizerType:                   authorizerType,
		authorizerTimeoutMilliseconds:    authorizerTimeoutMilliseconds,
		authorizerMaxInstructions:        authorizerMaxInstructions,
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
