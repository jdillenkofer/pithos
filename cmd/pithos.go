package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage"
	storageConfig "github.com/jdillenkofer/pithos/internal/storage/config"
	"github.com/jdillenkofer/pithos/internal/storage/migrator"
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
	  "dbPath": "./data/pithos.db"
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

const subcommandServe = "serve"
const subcommandMigrateStorage = "migrate-storage"

func main() {
	ctx := context.Background()
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s %s|%s [options]\n", os.Args[0], subcommandServe, subcommandMigrateStorage)
		os.Exit(1)
	}

	subcommand := os.Args[1]
	switch subcommand {
	case subcommandServe:
		serve(ctx)
	case subcommandMigrateStorage:
		migrateStorage(ctx)
	default:
		log.Fatalf("Invalid subcommand: %s. Expected one of '%s', '%s'.\n", subcommand, subcommandServe, subcommandMigrateStorage)
	}
}

func serve(ctx context.Context) {
	settings, err := settings.LoadSettings(os.Args[2:])
	if err != nil {
		log.Fatal("Error while loading settings: ", err)
	}

	dbContainer, store := loadStorageConfiguration(settings.StorageJsonPath())

	dbs := dbContainer.Dbs()

	err = store.Start(ctx)
	if err != nil {
		log.Fatal("Couldn't start storage: ", err)
	}

	defer func() {
		err := store.Stop(ctx)
		if err != nil {
			log.Fatal("Couldn't stop storage: ", err)
		}
		for _, db := range dbs {
			err = db.Close()
			if err != nil {
				log.Fatal("Couldn't close database: ", err)
			}
		}
	}()

	// TODO: Load this from a file
	authorizationCode := `
	function authorizeRequest(request)
	  return true
	end
	`
	requestAuthorizer := lua.NewLuaAuthorizer(authorizationCode)

	handler := server.SetupServer(settings.AccessKeyId(), settings.SecretAccessKey(), settings.Region(), settings.Domain(), requestAuthorizer, store)
	addr := fmt.Sprintf("%v:%v", settings.BindAddress(), settings.Port())
	httpServer := &http.Server{
		BaseContext: func(net.Listener) context.Context { return ctx },
		Addr:        addr,
		Handler:     handler,
	}

	if settings.MonitoringPortEnabled() {
		monitoringHandler := server.SetupMonitoringServer(dbs)
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
	}

	log.Printf("Listening with s3 api on http://%v\n", addr)
	log.Fatal(httpServer.ListenAndServe())
}

func loadStorageConfiguration(storageJsonPath string) (*config.DbContainer, storage.Storage) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		log.Fatal("Error while creating diContainer: ", err)
	}
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*prometheus.Registerer)(nil)), prometheus.DefaultRegisterer)
	if err != nil {
		log.Fatal("Error while registering prometheus.Registerer in diContainer: ", err)
	}

	dbContainer := config.NewDbContainer()
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*config.DbContainer)(nil)), dbContainer)
	if err != nil {
		log.Fatal("Error while registering dbContainer in diContainer: ", err)
	}

	storageJsonConfig, err := os.ReadFile(storageJsonPath)
	if err != nil {
		log.Println("Couldn't load storageJson: ", err)
		log.Println("Using defaultStorageConfig as fallback")
		storageJsonConfig = []byte(defaultStorageConfig)
	}

	storageInstantiator, err := storageConfig.CreateStorageInstantiatorFromJson(storageJsonConfig)
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
	return dbContainer, store
}

func migrateStorage(ctx context.Context) {
	if len(os.Args) < 4 {
		fmt.Printf("Usage: %s %s [source-config.json] [destination-config.json]\n", os.Args[0], subcommandMigrateStorage)
		os.Exit(1)
	}
	sourceStorageConfig := os.Args[2]
	destinationStorageConfig := os.Args[3]

	sourceDbContainer, sourceStorage := loadStorageConfiguration(sourceStorageConfig)

	sourceDbs := sourceDbContainer.Dbs()

	err := sourceStorage.Start(ctx)
	if err != nil {
		log.Fatal("Couldn't start storage: ", err)
	}

	defer func() {
		err := sourceStorage.Stop(ctx)
		if err != nil {
			log.Fatal("Couldn't stop storage: ", err)
		}
		for _, db := range sourceDbs {
			err = db.Close()
			if err != nil {
				log.Fatal("Couldn't close database:", err)
			}
		}
	}()

	destinationDbContainer, destinationStorage := loadStorageConfiguration(destinationStorageConfig)

	destinationDbs := destinationDbContainer.Dbs()

	err = destinationStorage.Start(ctx)
	if err != nil {
		log.Fatal("Couldn't start storage: ", err)
	}

	defer func() {
		err := destinationStorage.Stop(ctx)
		if err != nil {
			log.Fatal("Couldn't stop storage: ", err)
		}
		for _, db := range destinationDbs {
			err = db.Close()
			if err != nil {
				log.Fatal("Couldn't close database: ", err)
			}
		}
	}()

	log.Println("Storage migration started!")
	err = migrator.MigrateStorage(ctx, sourceStorage, destinationStorage)
	if err != nil {
		log.Fatal("Could not migrate storage: ", err)
	}
	log.Println("Storage migration successfully completed!")
}
