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

	storagePath := settings.StoragePath()
	db, err := storage.OpenDatabase(storagePath)
	if err != nil {
		log.Fatal("Couldn't open database")
	}
	storage := storage.CreateAndInitializeStorage(storagePath, db, settings.UseFilesystemBlobStore(), settings.WrapBlobStoreWithOutbox())
	defer func() {
		err := storage.Stop()
		if err != nil {
			log.Fatal("Couldn't stop storage")
		}
		err = db.Close()
		if err != nil {
			log.Fatal("Couldn't close database")
		}
	}()

	server := server.SetupServer(settings.Domain(), storage)
	addr := fmt.Sprintf("%v:%v", settings.BindAddress(), settings.Port())
	httpServer := &http.Server{Addr: addr, Handler: server}

	log.Printf("Listening with s3 api on http://%v\n", addr)
	log.Fatal(httpServer.ListenAndServe())
}
