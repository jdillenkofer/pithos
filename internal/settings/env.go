package settings

import (
	"os"
	"strconv"
	"strings"
)

const envKeyPrefix = "PITHOS"

const legacyAccessKeyIdEnvKey = envKeyPrefix + "_ACCESS_KEY_ID"
const legacySecretAccessKeyEnvKey = envKeyPrefix + "_SECRET_ACCESS_KEY"
const regionEnvKey = envKeyPrefix + "_REGION"
const domainEnvKey = envKeyPrefix + "_DOMAIN"
const bindAddressEnvKey = envKeyPrefix + "_BIND_ADDRESS"
const portEnvKey = envKeyPrefix + "_PORT"
const monitoringPortEnvKey = envKeyPrefix + "_MONITORING_PORT"
const monitoringPortEnabledEnvKey = envKeyPrefix + "_MONITORING_PORT_ENABLED"
const storageJsonPathEnvKey = envKeyPrefix + "_STORAGE_JSON_PATH"
const authorizerPathEnvKey = envKeyPrefix + "_AUTHORIZER_PATH"
const logLevelEnvKey = envKeyPrefix + "_LOG_LEVEL"

func getCredentialsFromEnv() []Credentials {
	// Check for legacy environment variables first
	accessKeyId := getStringFromEnv(legacyAccessKeyIdEnvKey)
	secretAccessKey := getStringFromEnv(legacySecretAccessKeyEnvKey)

	if accessKeyId != nil && secretAccessKey != nil {
		return []Credentials{
			{
				AccessKeyId:     *accessKeyId,
				SecretAccessKey: *secretAccessKey,
			},
		}
	}

	// Check for new environment variables
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

func loadSettingsFromEnv() (*Settings, error) {
	credentials := getCredentialsFromEnv()
	region := getStringFromEnv(regionEnvKey)
	domain := getStringFromEnv(domainEnvKey)
	bindAddress := getStringFromEnv(bindAddressEnvKey)
	port := getIntFromEnv(portEnvKey)
	monitoringPort := getIntFromEnv(monitoringPortEnvKey)
	monitoringPortEnabled := getBoolFromEnv(monitoringPortEnabledEnvKey)
	storageJsonPath := getStringFromEnv(storageJsonPathEnvKey)
	authorizerPath := getStringFromEnv(authorizerPathEnvKey)
	logLevel := getStringFromEnv(logLevelEnvKey)
	return &Settings{
		credentials:           credentials,
		region:                region,
		domain:                domain,
		bindAddress:           bindAddress,
		port:                  port,
		monitoringPort:        monitoringPort,
		monitoringPortEnabled: monitoringPortEnabled,
		storageJsonPath:       storageJsonPath,
		authorizerPath:        authorizerPath,
		logLevel:              logLevel,
	}, nil
}
