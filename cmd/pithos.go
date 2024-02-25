package main

import (
	"log"

	server "github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/storage"
)

func main() {
	protocol := "http"
	addr := "localhost:9000"
	server := server.New(addr, storage.NewMockStorage())
	log.Printf("Listening with s3 api on %v://%v\n", protocol, addr)
	log.Fatal(server.ListenAndServe())
}
