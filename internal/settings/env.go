package settings

import (
	"os"
	"strconv"
)

const envKeyPrefix string = "PITHOS"

const accessKeyIdEnvKey string = envKeyPrefix + "_ACCESS_KEY_ID"
const secretAccessKeyEnvKey string = envKeyPrefix + "_SECRET_ACCESS_KEY"
const regionEnvKey string = envKeyPrefix + "_REGION"
const domainEnvKey string = envKeyPrefix + "_DOMAIN"
const bindAddressEnvKey string = envKeyPrefix + "_BIND_ADDRESS"
const portEnvKey string = envKeyPrefix + "_PORT"
const monitoringPortEnvKey string = envKeyPrefix + "_MONITORING_PORT"
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

func loadSettingsFromEnv() (*Settings, error) {
	accessKeyId := getStringFromEnv(accessKeyIdEnvKey)
	secretAccessKey := getStringFromEnv(secretAccessKeyEnvKey)
	region := getStringFromEnv(regionEnvKey)
	domain := getStringFromEnv(domainEnvKey)
	bindAddress := getStringFromEnv(bindAddressEnvKey)
	port := getIntFromEnv(portEnvKey)
	monitoringPort := getIntFromEnv(monitoringPortEnvKey)
	storageJsonPath := getStringFromEnv(storageJsonPathEnvKey)
	return &Settings{
		accessKeyId:     accessKeyId,
		secretAccessKey: secretAccessKey,
		region:          region,
		domain:          domain,
		bindAddress:     bindAddress,
		port:            port,
		monitoringPort:  monitoringPort,
		storageJsonPath: storageJsonPath,
	}, nil
}
