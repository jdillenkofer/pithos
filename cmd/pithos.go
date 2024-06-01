package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	server "github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	protocol := "http"
	baseDomain := "localhost"
	bindAddress := "0.0.0.0"
	addr := fmt.Sprintf("%v:9000", bindAddress)
	storagePath := "./data"
	err := os.MkdirAll(storagePath, os.ModePerm)
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
		log.Fatal(err)
	}
	blobStore, err := blob.NewSqlBlobStore(db)
	if err != nil {
		log.Fatal(err)
	}
	/*
		blobStore, err := blob.NewFilesystemBlobStore(filepath.Join(storagePath, "blobs"))
		if err != nil {
			log.Fatal(err)
		}
	*/
	storage, err := storage.NewMetadataBlobStorage(metadataStore, blobStore)
	if err != nil {
		log.Fatal(err)
	}
	server := server.SetupServer(baseDomain, storage)
	httpServer := &http.Server{Addr: addr, Handler: server}

	log.Printf("Listening with s3 api on %v://%v\n", protocol, addr)
	log.Fatal(httpServer.ListenAndServe())
}
