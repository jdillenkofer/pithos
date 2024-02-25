package main

import (
	"log"

	server "github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/storage"
)

func main() {
	protocol := "http"
	addr := "localhost:9000"
	storage, err := storage.NewFilesystemStorage("./data")
	if err != nil {
		log.Fatal(err)
	}
	server := server.New(addr, storage)
	log.Printf("Listening with s3 api on %v://%v\n", protocol, addr)
	log.Fatal(server.ListenAndServe())
}
