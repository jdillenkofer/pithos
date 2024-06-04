package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	server "github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
	_ "github.com/mattn/go-sqlite3"
)

func main() {

	settings, err := settings.LoadSettings()
	if err != nil {
		log.Fatal("Error while loading settings: ", err)
	}

	storagePath := settings.StoragePath()
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
	metadataStore, err := metadata.NewSqlMetadataStore()
	if err != nil {
		log.Fatal("Error during NewSqlMetadataStore: ", err)
	}
	var blobStore blob.BlobStore
	if settings.UseFilesystemBlobStore() {
		blobStore, err = blob.NewFilesystemBlobStore(filepath.Join(storagePath, "blobs"))
		if err != nil {
			log.Fatal("Error during NewFilesystemBlobStore: ", err)
		}
	} else {
		blobStore, err = blob.NewSqlBlobStore()
		if err != nil {
			log.Fatal("Error during NewSqlBlobStore: ", err)
		}
	}
	storage, err := storage.NewMetadataBlobStorage(db, metadataStore, blobStore)
	if err != nil {
		log.Fatal("Error during NewMetadataBlobStorage: ", err)
	}

	domain := settings.Domain()
	server := server.SetupServer(domain, storage)
	addr := fmt.Sprintf("%v:%v", settings.BindAddress(), settings.Port())
	httpServer := &http.Server{Addr: addr, Handler: server}

	log.Printf("Listening with s3 api on http://%v\n", addr)
	log.Fatal(httpServer.ListenAndServe())
}
