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
const monitoringPortEnvKey = envKeyPrefix + "_MONITORING_PORT"
const monitoringPortEnabledEnvKey = envKeyPrefix + "_MONITORING_PORT_ENABLED"
const storageJsonPathEnvKey = envKeyPrefix + "_STORAGE_JSON_PATH"
const authorizerPathEnvKey = envKeyPrefix + "_AUTHORIZER_PATH"
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

func loadSettingsFromEnv() (*Settings, error) {
	credentials := getCredentialsFromEnv()
	authenticationEnabled := getBoolFromEnv(authenticationEnabledEnvKey)
	region := getStringFromEnv(regionEnvKey)
	domain := getStringFromEnv(domainEnvKey)
	websiteDomain := getStringFromEnv(websiteDomainEnvKey)
	bindAddress := getStringFromEnv(bindAddressEnvKey)
	port := getIntFromEnv(portEnvKey)
	monitoringPort := getIntFromEnv(monitoringPortEnvKey)
	monitoringPortEnabled := getBoolFromEnv(monitoringPortEnabledEnvKey)
	storageJsonPath := getStringFromEnv(storageJsonPathEnvKey)
	authorizerPath := getStringFromEnv(authorizerPathEnvKey)
	trustForwardedHeaders := getBoolFromEnv(trustForwardedHeadersEnvKey)
	trustedProxyCIDRs := getStringSliceFromEnv(trustedProxyCIDRsEnvKey)
	logLevel := getStringFromEnv(logLevelEnvKey)
	otelEnabled := getBoolFromEnv(otelEnabledEnvKey)
	otelExporter := getStringFromEnv(otelExporterEnvKey)
	otelEndpoint := getStringFromEnv(otelEndpointEnvKey)

	return &Settings{
		authenticationEnabled: authenticationEnabled,
		credentials:           credentials,
		region:                region,
		domain:                domain,
		websiteDomain:         websiteDomain,
		bindAddress:           bindAddress,
		port:                  port,
		monitoringPort:        monitoringPort,
		monitoringPortEnabled: monitoringPortEnabled,
		storageJsonPath:       storageJsonPath,
		authorizerPath:        authorizerPath,
		trustForwardedHeaders: trustForwardedHeaders,
		trustedProxyCIDRs:     trustedProxyCIDRs,
		logLevel:              logLevel,
		otelEnabled:           otelEnabled,
		otelExporter:          otelExporter,
		otelEndpoint:          otelEndpoint,
	}, nil
}
