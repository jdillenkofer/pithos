package main

import (
	"database/sql"
	"log"
	"net/http"
	"path/filepath"

	server "github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	protocol := "http"
	addr := "localhost:9000"
	db, err := sql.Open("sqlite3", filepath.Join("./data", "metadata.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	metadataStore, err := metadata.NewSqlMetadataStore(db)
	if err != nil {
		log.Fatal(err)
	}
	blobStore, err := blob.NewFilesystemBlobStore("./data/blobs")
	if err != nil {
		log.Fatal(err)
	}
	storage, err := storage.NewMetadataBlobStorage(metadataStore, blobStore)
	if err != nil {
		log.Fatal(err)
	}
	server := server.SetupServer(storage)
	httpServer := &http.Server{Addr: addr, Handler: server}

	log.Printf("Listening with s3 api on %v://%v\n", protocol, addr)
	log.Fatal(httpServer.ListenAndServe())
}
