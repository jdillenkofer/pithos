package settings

import (
	"os"
	"strings"
)

const envKeyPrefix string = "PITHOS"

const domainEnvKey string = envKeyPrefix + "_DOMAIN"
const bindAddressEnvKey string = envKeyPrefix + "_BIND_ADDRESS"
const portEnvKey string = envKeyPrefix + "_PORT"
const storagePathEnvKey string = envKeyPrefix + "_STORAGE_PATH"
const useFilesystemBlobStoreEnvKey string = envKeyPrefix + "_USE_FILESYSTEM_BLOB_STORE"

func getStringFromEnv(envKey string) *string {
	val := os.Getenv(envKey)
	if val == "" {
		return nil
	}
	return &val
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
	domain := getStringFromEnv(domainEnvKey)
	bindAddress := getStringFromEnv(bindAddressEnvKey)
	port := getStringFromEnv(portEnvKey)
	storagePath := getStringFromEnv(storagePathEnvKey)
	useFilesystemBlobStore := getBoolFromEnv(useFilesystemBlobStoreEnvKey)
	return &Settings{
		domain:                 domain,
		bindAddress:            bindAddress,
		port:                   port,
		storagePath:            storagePath,
		useFilesystemBlobStore: useFilesystemBlobStore,
	}, nil
}
