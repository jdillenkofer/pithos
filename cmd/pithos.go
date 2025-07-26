package main

import (
	"context"
	"fmt"
	"log/slog"
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

const defaultAuthorizationCode = `
function authorizeRequest(request)
  return true
end
`

const subcommandServe = "serve"
const subcommandMigrateStorage = "migrate-storage"

func main() {
	var programLevel = new(slog.LevelVar)
	programLevel.Set(slog.LevelDebug)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     programLevel,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()
	if len(os.Args) < 2 {
		slog.Info(fmt.Sprintf("Usage: %s %s|%s [options]\n", os.Args[0], subcommandServe, subcommandMigrateStorage))
		os.Exit(1)
	}

	subcommand := os.Args[1]
	switch subcommand {
	case subcommandServe:
		serve(ctx)
	case subcommandMigrateStorage:
		migrateStorage(ctx)
	default:
		slog.Error(fmt.Sprintf("Invalid subcommand: %s. Expected one of '%s', '%s'.\n", subcommand, subcommandServe, subcommandMigrateStorage))
		os.Exit(1)
	}
}

func serve(ctx context.Context) {
	settings, err := settings.LoadSettings(os.Args[2:])
	if err != nil {
		slog.Error(fmt.Sprint("Error while loading settings: ", err))
		os.Exit(1)
	}

	dbContainer, store := loadStorageConfiguration(settings.StorageJsonPath())

	dbs := dbContainer.Dbs()

	err = store.Start(ctx)
	if err != nil {
		slog.Error(fmt.Sprint("Couldn't start storage: ", err))
		os.Exit(1)
	}

	defer func() {
		err := store.Stop(ctx)
		if err != nil {
			slog.Error(fmt.Sprint("Couldn't stop storage: ", err))
			os.Exit(1)
		}
		for _, db := range dbs {
			err = db.Close()
			if err != nil {
				slog.Error(fmt.Sprint("Couldn't close database: ", err))
				os.Exit(1)
			}
		}
	}()

	requestAuthorizer, err := loadRequestAuthorizer(settings.AuthorizerPath())
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create LuaAuthorizer: %s", err))
	}

	handler := server.SetupServer(settings.Credentials(), settings.Region(), settings.Domain(), requestAuthorizer, store)
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
			slog.Info(fmt.Sprintf("Listening with monitoring api on http://%v\n", monitoringAddr))
			httpMonitoringServer.ListenAndServe()
		})()
	}

	slog.Info(fmt.Sprintf("Listening with s3 api on http://%v\n", addr))
	err = httpServer.ListenAndServe()
	if err != nil {
		slog.Error(fmt.Sprintf("Error while starting http server: %s", err))
		os.Exit(1)
	}
}

func loadRequestAuthorizer(authorizerPath string) (*lua.LuaAuthorizer, error) {
	authorizerCode, err := os.ReadFile(authorizerPath)
	if err != nil {
		slog.Warn(fmt.Sprint("Couldn't load authorizer: ", err))
		slog.Warn("Using defaultAuthorizationCode (which allows every operation) as fallback")
		authorizerCode = []byte(defaultAuthorizationCode)
	}
	return lua.NewLuaAuthorizer(string(authorizerCode))
}

func loadStorageConfiguration(storageJsonPath string) (*config.DbContainer, storage.Storage) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		slog.Error(fmt.Sprint("Error while creating diContainer: ", err))
		os.Exit(1)
	}
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*prometheus.Registerer)(nil)), prometheus.DefaultRegisterer)
	if err != nil {
		slog.Error(fmt.Sprint("Error while registering prometheus.Registerer in diContainer: ", err))
		os.Exit(1)
	}

	dbContainer := config.NewDbContainer()
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*config.DbContainer)(nil)), dbContainer)
	if err != nil {
		slog.Error(fmt.Sprint("Error while registering dbContainer in diContainer: ", err))
		os.Exit(1)
	}

	storageJsonConfig, err := os.ReadFile(storageJsonPath)
	if err != nil {
		slog.Warn(fmt.Sprint("Couldn't load storageJson: ", err))
		slog.Warn("Using defaultStorageConfig as fallback")
		storageJsonConfig = []byte(defaultStorageConfig)
	}

	storageInstantiator, err := storageConfig.CreateStorageInstantiatorFromJson(storageJsonConfig)
	if err != nil {
		slog.Error(fmt.Sprint("Error while creating storageInstantiator from json: ", err))
		os.Exit(1)
	}
	err = storageInstantiator.RegisterReferences(diContainer)
	if err != nil {
		slog.Error(fmt.Sprint("Error while registering references: ", err))
		os.Exit(1)
	}
	store, err := storageInstantiator.Instantiate(diContainer)
	if err != nil {
		slog.Error(fmt.Sprint("Error while instantiating storage: ", err))
		os.Exit(1)
	}
	return dbContainer, store
}

func migrateStorage(ctx context.Context) {
	if len(os.Args) < 4 {
		slog.Info(fmt.Sprintf("Usage: %s %s [source-config.json] [destination-config.json]\n", os.Args[0], subcommandMigrateStorage))
		os.Exit(1)
	}
	sourceStorageConfig := os.Args[2]
	destinationStorageConfig := os.Args[3]

	sourceDbContainer, sourceStorage := loadStorageConfiguration(sourceStorageConfig)

	sourceDbs := sourceDbContainer.Dbs()

	err := sourceStorage.Start(ctx)
	if err != nil {
		slog.Error(fmt.Sprint("Couldn't start storage: ", err))
		os.Exit(1)
	}

	defer func() {
		err := sourceStorage.Stop(ctx)
		if err != nil {
			slog.Error(fmt.Sprint("Couldn't stop storage: ", err))
			os.Exit(1)
		}
		for _, db := range sourceDbs {
			err = db.Close()
			if err != nil {
				slog.Error(fmt.Sprint("Couldn't close database:", err))
				os.Exit(1)
			}
		}
	}()

	destinationDbContainer, destinationStorage := loadStorageConfiguration(destinationStorageConfig)

	destinationDbs := destinationDbContainer.Dbs()

	err = destinationStorage.Start(ctx)
	if err != nil {
		slog.Error(fmt.Sprint("Couldn't start storage: ", err))
		os.Exit(1)
	}

	defer func() {
		err := destinationStorage.Stop(ctx)
		if err != nil {
			slog.Error(fmt.Sprint("Couldn't stop storage: ", err))
			os.Exit(1)
		}
		for _, db := range destinationDbs {
			err = db.Close()
			if err != nil {
				slog.Error(fmt.Sprint("Couldn't close database: ", err))
				os.Exit(1)
			}
		}
	}()

	slog.Info("Storage migration started!")
	err = migrator.MigrateStorage(ctx, sourceStorage, destinationStorage)
	if err != nil {
		slog.Error(fmt.Sprint("Could not migrate storage: ", err))
		os.Exit(1)
	}
	slog.Info("Storage migration successfully completed!")
}
