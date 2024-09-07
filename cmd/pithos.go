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
	"github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage"
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
	db, err := storage.OpenDatabase(storagePath)
	if err != nil {
		log.Fatal("Couldn't open database")
	}
	store := storage.CreateStorage(storagePath, db, settings.UseFilesystemBlobStore(), settings.UseEncryptedBlobStore(), settings.WrapBlobStoreWithOutbox())

	replication := settings.Replication()
	if replication != nil {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(replication.Region()), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(replication.AccessKeyId(), replication.SecretAccessKey(), "")))

		if err != nil {
			log.Fatal("Couldn't create s3Client config")
		}

		s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = replication.Endpoint()
		})

		var s3ClientStorage storage.Storage
		s3ClientStorage, err = storage.NewS3ClientStorage(s3Client)
		if err != nil {
			log.Fatal("Could not create s3ClientStorage")
		}
		if replication.UseOutbox() {
			s3ClientStorage, err = storage.NewOutboxStorage(db, s3ClientStorage)
			if err != nil {
				log.Fatal("Could not create outboxStorage")
			}
		}
		store, err = storage.NewReplicationStorage(store, s3ClientStorage)
		if err != nil {
			log.Fatal("Could not create replicationStorage")
		}
	}

	store, err = storage.NewPrometheusStorageMiddleware(store, prometheus.DefaultRegisterer)
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

	metricHandler := server.SetupMetricServer()
	metricsAddr := fmt.Sprintf("%v:%v", settings.BindAddress(), settings.MetricPort())
	httpMetricServer := &http.Server{
		BaseContext: func(net.Listener) context.Context { return ctx },
		Addr:        metricsAddr,
		Handler:     metricHandler,
	}
	go (func() {
		log.Printf("Listening with metrics api on http://%v\n", metricsAddr)
		httpMetricServer.ListenAndServe()
	})()

	log.Printf("Listening with s3 api on http://%v\n", addr)
	log.Fatal(httpServer.ListenAndServe())
}
