package settings

import (
	"os"
	"strconv"
	"strings"
)

const envKeyPrefix string = "PITHOS"
const replicationEnvKeyPrefix string = envKeyPrefix + "_REPLICATION"

const accessKeyIdEnvKey string = envKeyPrefix + "_ACCESS_KEY_ID"
const secretAccessKeyEnvKey string = envKeyPrefix + "_SECRET_ACCESS_KEY"
const regionEnvKey string = envKeyPrefix + "_REGION"
const domainEnvKey string = envKeyPrefix + "_DOMAIN"
const bindAddressEnvKey string = envKeyPrefix + "_BIND_ADDRESS"
const portEnvKey string = envKeyPrefix + "_PORT"
const storagePathEnvKey string = envKeyPrefix + "_STORAGE_PATH"
const useFilesystemBlobStoreEnvKey string = envKeyPrefix + "_USE_FILESYSTEM_BLOB_STORE"
const wrapBlobStoreWithOutboxEnvKey string = envKeyPrefix + "_WRAP_BLOB_STORE_WITH_OUTBOX"
const replicationAccessKeyIdEnvKey string = replicationEnvKeyPrefix + "_ACCESS_KEY_ID"
const replicationSecretAccessKeyEnvKey string = replicationEnvKeyPrefix + "_SECRET_ACCESS_KEY"
const replicationRegionEnvKey string = replicationEnvKeyPrefix + "_REGION"
const replicationEndpointEnvKey string = replicationEnvKeyPrefix + "_ENDPOINT"
const replicationUseOutboxEnvKey string = replicationEnvKeyPrefix + "_USE_OUTBOX"

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

func loadReplicationSettingsFromEnv() (*ReplicationSettings, error) {
	accessKeyId := getStringFromEnv(replicationAccessKeyIdEnvKey)
	secretAccessKey := getStringFromEnv(replicationSecretAccessKeyEnvKey)
	region := getStringFromEnv(replicationRegionEnvKey)
	endpoint := getStringFromEnv(replicationEndpointEnvKey)
	useOutbox := getBoolFromEnv(replicationUseOutboxEnvKey)
	if accessKeyId != nil && secretAccessKey != nil && region != nil {
		return &ReplicationSettings{
			accessKeyId:     accessKeyId,
			secretAccessKey: secretAccessKey,
			region:          region,
			endpoint:        endpoint,
			useOutbox:       useOutbox,
		}, nil
	}
	return nil, nil
}

func loadSettingsFromEnv() (*Settings, error) {
	accessKeyId := getStringFromEnv(accessKeyIdEnvKey)
	secretAccessKey := getStringFromEnv(secretAccessKeyEnvKey)
	region := getStringFromEnv(regionEnvKey)
	domain := getStringFromEnv(domainEnvKey)
	bindAddress := getStringFromEnv(bindAddressEnvKey)
	port := getIntFromEnv(portEnvKey)
	storagePath := getStringFromEnv(storagePathEnvKey)
	useFilesystemBlobStore := getBoolFromEnv(useFilesystemBlobStoreEnvKey)
	wrapBlobStoreWithOutbox := getBoolFromEnv(wrapBlobStoreWithOutboxEnvKey)
	replication, err := loadReplicationSettingsFromEnv()
	if err != nil {
		return nil, err
	}
	return &Settings{
		accessKeyId:             accessKeyId,
		secretAccessKey:         secretAccessKey,
		region:                  region,
		domain:                  domain,
		bindAddress:             bindAddress,
		port:                    port,
		storagePath:             storagePath,
		useFilesystemBlobStore:  useFilesystemBlobStore,
		wrapBlobStoreWithOutbox: wrapBlobStoreWithOutbox,
		replication:             replication,
	}, nil
}
