package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	store := storage.CreateStorage(storagePath, db, settings.UseFilesystemBlobStore(), settings.WrapBlobStoreWithOutbox())

	replication := settings.Replication()
	if replication != nil {
		cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(replication.Region()), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(replication.AccessKeyId(), replication.SecretAccessKey(), "")))

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

	err = store.Start()
	if err != nil {
		log.Fatal("Couldn't start storage")
	}

	defer func() {
		err := store.Stop()
		if err != nil {
			log.Fatal("Couldn't stop storage")
		}
		err = db.Close()
		if err != nil {
			log.Fatal("Couldn't close database")
		}
	}()

	server := server.SetupServer(settings.AccessKeyId(), settings.SecretAccessKey(), settings.Region(), settings.Domain(), store)
	addr := fmt.Sprintf("%v:%v", settings.BindAddress(), settings.Port())
	httpServer := &http.Server{Addr: addr, Handler: server}

	log.Printf("Listening with s3 api on http://%v\n", addr)
	log.Fatal(httpServer.ListenAndServe())
}
