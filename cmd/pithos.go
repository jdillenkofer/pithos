package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	server "github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
	_ "github.com/mattn/go-sqlite3"
)

const envKeyPrefix string = "PITHOS"

const domainEnvKey string = envKeyPrefix + "_DOMAIN"
const bindAddressEnvKey string = envKeyPrefix + "_BIND_ADDRESS"
const portEnvKey string = envKeyPrefix + "_PORT"
const storagePathEnvKey string = envKeyPrefix + "_STORAGE_PATH"
const useFilesystemBlobStoreEnvKey string = envKeyPrefix + "_USE_FILESYSTEM_BLOB_STORE"

const defaultDomain = "localhost"
const defaultBindAddress = "0.0.0.0"
const defaultPort = "9000"
const defaultStoragePath = "./data"
const defaultUseFilesystemBlobStore = false

type Settings struct {
	domain                 string
	bindAddress            string
	port                   string
	storagePath            string
	useFilesystemBlobStore bool
}

func getStringFromEnv(envKey string, defaultValue string) string {
	val := os.Getenv(envKey)
	if val == "" {
		return defaultValue
	}
	return val
}

func getBoolFromEnv(envKey string, defaultValue bool) bool {
	val := os.Getenv(envKey)
	val = strings.ToLower(val)
	if val == "" {
		return defaultValue
	}
	return val == "1" || val == "t" || val == "true"
}

func loadSettingsFromEnv() (*Settings, error) {
	domain := getStringFromEnv(domainEnvKey, defaultDomain)
	bindAddress := getStringFromEnv(bindAddressEnvKey, defaultBindAddress)
	port := getStringFromEnv(portEnvKey, defaultPort)
	storagePath := getStringFromEnv(storagePathEnvKey, defaultStoragePath)
	useFilesystemBlobStore := getBoolFromEnv(useFilesystemBlobStoreEnvKey, defaultUseFilesystemBlobStore)
	return &Settings{
		domain:                 domain,
		bindAddress:            bindAddress,
		port:                   port,
		storagePath:            storagePath,
		useFilesystemBlobStore: useFilesystemBlobStore,
	}, nil
}

func main() {

	settings, err := loadSettingsFromEnv()
	if err != nil {
		log.Fatal("Error while loading settings: ", err)
	}

	storagePath := settings.storagePath
	err = os.MkdirAll(storagePath, os.ModePerm)
	if err != nil {
		log.Fatal("Error while creating data directory: ", err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(storagePath, "pithos.db"))
	if err != nil {
		log.Fatal("Error when opening sqlite database: ", err)
	}
	defer db.Close()
	err = storage.SetupDatabase(db)
	if err != nil {
		log.Fatal("Error during SetupDatabase: ", err)
	}
	metadataStore, err := metadata.NewSqlMetadataStore(db)
	if err != nil {
		log.Fatal("Error during NewSqlMetadataStore: ", err)
	}
	var blobStore blob.BlobStore
	if settings.useFilesystemBlobStore {
		blobStore, err = blob.NewFilesystemBlobStore(filepath.Join(storagePath, "blobs"))
		if err != nil {
			log.Fatal("Error during NewFilesystemBlobStore: ", err)
		}
	} else {
		blobStore, err = blob.NewSqlBlobStore(db)
		if err != nil {
			log.Fatal("Error during NewSqlBlobStore: ", err)
		}
	}
	storage, err := storage.NewMetadataBlobStorage(metadataStore, blobStore)
	if err != nil {
		log.Fatal("Error during NewMetadataBlobStorage: ", err)
	}

	domain := settings.domain
	server := server.SetupServer(domain, storage)
	addr := fmt.Sprintf("%v:%v", settings.bindAddress, settings.port)
	httpServer := &http.Server{Addr: addr, Handler: server}

	log.Printf("Listening with s3 api on http://%v\n", addr)
	log.Fatal(httpServer.ListenAndServe())
}
