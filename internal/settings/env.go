package settings

import (
	"os"
	"strconv"
	"strings"
)

const envKeyPrefix = "PITHOS"

const accessKeyIdEnvKey = envKeyPrefix + "_ACCESS_KEY_ID"
const secretAccessKeyEnvKey = envKeyPrefix + "_SECRET_ACCESS_KEY"
const regionEnvKey = envKeyPrefix + "_REGION"
const domainEnvKey = envKeyPrefix + "_DOMAIN"
const bindAddressEnvKey = envKeyPrefix + "_BIND_ADDRESS"
const portEnvKey = envKeyPrefix + "_PORT"
const monitoringPortEnvKey = envKeyPrefix + "_MONITORING_PORT"
const monitoringPortEnabledEnvKey = envKeyPrefix + "_MONITORING_PORT_ENABLED"
const storageJsonPathEnvKey = envKeyPrefix + "_STORAGE_JSON_PATH"
const authorizerPathEnvKey = envKeyPrefix + "_AUTHORIZER_PATH"

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
	storageJsonPath := getStringFromEnv(storageJsonPathEnvKey)
	authorizerPath := getStringFromEnv(authorizerPathEnvKey)
	return &Settings{
		accessKeyId:           accessKeyId,
		secretAccessKey:       secretAccessKey,
		region:                region,
		domain:                domain,
		bindAddress:           bindAddress,
		port:                  port,
		monitoringPort:        monitoringPort,
		monitoringPortEnabled: monitoringPortEnabled,
		storageJsonPath:       storageJsonPath,
		authorizerPath:        authorizerPath,
	}, nil
}
