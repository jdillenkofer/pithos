package settings

import (
	"os"
	"strconv"
	"strings"
)

const envKeyPrefix string = "PITHOS"

const accessKeyIdEnvKey string = envKeyPrefix + "_ACCESS_KEY_ID"
const secretAccessKeyEnvKey string = envKeyPrefix + "_SECRET_ACCESS_KEY"
const regionEnvKey string = envKeyPrefix + "_REGION"
const domainEnvKey string = envKeyPrefix + "_DOMAIN"
const bindAddressEnvKey string = envKeyPrefix + "_BIND_ADDRESS"
const portEnvKey string = envKeyPrefix + "_PORT"
const monitoringPortEnvKey string = envKeyPrefix + "_MONITORING_PORT"
const monitoringPortEnabledEnvKey string = envKeyPrefix + "_MONITORING_PORT_ENABLED"
const compressionEnabledEnvKey string = envKeyPrefix + "_COMPRESSION_ENABLED"
const storageJsonPathEnvKey string = envKeyPrefix + "_STORAGE_JSON_PATH"

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
	accessKeyId := getStringFromEnv(accessKeyIdEnvKey)
	secretAccessKey := getStringFromEnv(secretAccessKeyEnvKey)
	region := getStringFromEnv(regionEnvKey)
	domain := getStringFromEnv(domainEnvKey)
	bindAddress := getStringFromEnv(bindAddressEnvKey)
	port := getIntFromEnv(portEnvKey)
	monitoringPort := getIntFromEnv(monitoringPortEnvKey)
	monitoringPortEnabled := getBoolFromEnv(monitoringPortEnabledEnvKey)
	compressionEnabled := getBoolFromEnv(compressionEnabledEnvKey)
	storageJsonPath := getStringFromEnv(storageJsonPathEnvKey)
	return &Settings{
		accessKeyId:           accessKeyId,
		secretAccessKey:       secretAccessKey,
		region:                region,
		domain:                domain,
		bindAddress:           bindAddress,
		port:                  port,
		monitoringPort:        monitoringPort,
		monitoringPortEnabled: monitoringPortEnabled,
		compressionEnabled:    compressionEnabled,
		storageJsonPath:       storageJsonPath,
	}, nil
}
