package main

import (
	"log"
	"net/http"

	server "github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
)

func main() {
	protocol := "http"
	addr := "localhost:9000"
	metadataStore, err := metadata.NewJsonMetadataStore("./data/metadata")
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
