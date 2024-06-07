package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	settings, err := settings.LoadSettings()
	if err != nil {
		log.Fatal("Error while loading settings: ", err)
	}

	storage, closeStorage := storage.CreateAndInitializeStorage(settings.StoragePath(), settings.UseFilesystemBlobStore(), settings.WrapBlobStoreWithOutbox())
	defer closeStorage()

	server := server.SetupServer(settings.Domain(), storage)
	addr := fmt.Sprintf("%v:%v", settings.BindAddress(), settings.Port())
	httpServer := &http.Server{Addr: addr, Handler: server}

	log.Printf("Listening with s3 api on http://%v\n", addr)
	log.Fatal(httpServer.ListenAndServe())
}
