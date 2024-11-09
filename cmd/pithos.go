package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteStorageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry/sqlite"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	prometheusStorageMiddleware "github.com/jdillenkofer/pithos/internal/storage/middlewares/prometheus"
	"github.com/jdillenkofer/pithos/internal/storage/outbox"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
	"github.com/jdillenkofer/pithos/internal/storage/s3client"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	ctx := context.Background()
	settings, err := settings.LoadSettings()
	if err != nil {
		log.Fatal("Error while loading settings: ", err)
	}

	storagePath := settings.StoragePath()
	db, err := database.OpenDatabase(storagePath)
	if err != nil {
		log.Fatal("Couldn't open database")
	}
	store := storageFactory.CreateStorage(storagePath, db, settings.UseFilesystemBlobStore(), settings.BlobStoreEncryptionPassword(), settings.WrapBlobStoreWithOutbox())

	replicationSettings := settings.Replication()
	if replicationSettings != nil {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(replicationSettings.Region()), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(replicationSettings.AccessKeyId(), replicationSettings.SecretAccessKey(), "")))

		if err != nil {
			log.Fatal("Couldn't create s3Client config")
		}

		s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = replicationSettings.Endpoint()
		})

		var s3ClientStorage storage.Storage
		s3ClientStorage, err = s3client.NewStorage(s3Client)
		if err != nil {
			log.Fatal("Could not create s3ClientStorage")
		}
		if replicationSettings.UseOutbox() {
			storageOutboxEntryRepository, err := sqliteStorageOutboxEntry.NewRepository(db)
			if err != nil {
				log.Fatalf("Could not create StorageOutboxEntryRepository: %s", err)

			}
			s3ClientStorage, err = outbox.NewStorage(db, s3ClientStorage, storageOutboxEntryRepository)
			if err != nil {
				log.Fatal("Could not create outboxStorage")
			}
		}
		store, err = replication.NewStorage(store, s3ClientStorage)
		if err != nil {
			log.Fatal("Could not create replicationStorage")
		}
	}

	store, err = prometheusStorageMiddleware.NewStorageMiddleware(store, prometheus.DefaultRegisterer)
	if err != nil {
		log.Fatal("Could not create prometheusStorageMiddleware")
	}

	err = store.Start(ctx)
	if err != nil {
		log.Fatal("Couldn't start storage")
	}

	defer func() {
		err := store.Stop(ctx)
		if err != nil {
			log.Fatal("Couldn't stop storage")
		}
		err = db.Close()
		if err != nil {
			log.Fatal("Couldn't close database")
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
