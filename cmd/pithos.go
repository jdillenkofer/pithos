package main

import (
	"log"
	"net/http"

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
	server := server.SetupServer(storage)
	httpServer := &http.Server{Addr: addr, Handler: server}

	log.Printf("Listening with s3 api on %v://%v\n", protocol, addr)
	log.Fatal(httpServer.ListenAndServe())
}
