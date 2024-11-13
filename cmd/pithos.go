package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"

	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage/config"
	dbConfig "github.com/jdillenkofer/pithos/internal/storage/database/config"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
)

const defaultStorageConfig = `
{
  "type": "MetadataBlobStorage",
  "db": {
    "type": "RegisterDatabaseReference",
	"refName": "db",
	"db": {
      "type": "SqliteDatabase",
	  "storagePath": "./data"
	}
  },
  "metadataStore": {
    "type": "SqlMetadataStore",
	"db": {
	  "type": "DatabaseReference",
	  "refName": "db"
	}
  },
  "blobStore": {
    "type": "SqlBlobStore",
	"db": {
	  "type": "DatabaseReference",
	  "refName": "db"
	}
  }
}
`

func main() {
	ctx := context.Background()
	settings, err := settings.LoadSettings()
	if err != nil {
		log.Fatal("Error while loading settings: ", err)
	}

	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		log.Fatal("Error while creating diContainer: ", err)
	}
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*prometheus.Registerer)(nil)), prometheus.DefaultRegisterer)
	if err != nil {
		log.Fatal("Error while registering prometheus.Registerer in diContainer: ", err)
	}

	storageConfig, err := os.ReadFile(settings.StorageJsonPath())
	if err != nil {
		storageConfig = []byte(defaultStorageConfig)
	}

	storageInstantiator, err := config.CreateStorageInstantiatorFromJson(storageConfig)
	if err != nil {
		log.Fatal("Error while creating storageInstantiator from json: ", err)
	}
	err = storageInstantiator.RegisterReferences(diContainer)
	if err != nil {
		log.Fatal("Error while registering references: ", err)
	}
	store, err := storageInstantiator.Instantiate(diContainer)
	if err != nil {
		log.Fatal("Error while instantiating storage: ", err)
	}

	di, err := diContainer.LookupByName("db")
	if err != nil {
		log.Fatal("Error expected primary db with reference name \"db\": ", err)
	}
	databaseInstantiator := di.(dbConfig.DatabaseInstantiator)
	db, err := databaseInstantiator.Instantiate(diContainer)
	if err != nil {
		log.Fatal("Error expected db instantiation: ", err)
	}

	err = store.Start(ctx)
	if err != nil {
		log.Fatal("Couldn't start storage: ", err)
	}

	defer func() {
		err := store.Stop(ctx)
		if err != nil {
			log.Fatal("Couldn't stop storage: ", err)
		}
		err = db.Close()
		if err != nil {
			log.Fatal("Couldn't close database:", err)
		}
	}()

	handler := server.SetupServer(settings.AccessKeyId(), settings.SecretAccessKey(), settings.Region(), settings.Domain(), store)
	addr := fmt.Sprintf("%v:%v", settings.BindAddress(), settings.Port())
	httpServer := &http.Server{
		BaseContext: func(net.Listener) context.Context { return ctx },
		Addr:        addr,
		Handler:     handler,
	}

	monitoringHandler := server.SetupMonitoringServer(db)
	monitoringAddr := fmt.Sprintf("%v:%v", settings.BindAddress(), settings.MonitoringPort())
	httpMonitoringServer := &http.Server{
		BaseContext: func(net.Listener) context.Context { return ctx },
		Addr:        monitoringAddr,
		Handler:     monitoringHandler,
	}
	go (func() {
		log.Printf("Listening with monitoring api on http://%v\n", monitoringAddr)
		httpMonitoringServer.ListenAndServe()
	})()

	log.Printf("Listening with s3 api on http://%v\n", addr)
	log.Fatal(httpServer.ListenAndServe())
}
