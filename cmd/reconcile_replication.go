package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
)

func reconcileReplication(ctx context.Context, args []string) (resultErr error) {
	flags := flag.NewFlagSet("reconcile-replication", flag.ContinueOnError)
	configPath := flags.String("storage-config", "", "Storage configuration file (required)")
	id := flags.String("replication-id", "", "Stable replication ID (required)")
	selection := flags.String("bucket", "", "Comma-separated bucket names; omitted selects all primary buckets")
	dryRun := flags.Bool("dry-run", false, "List missing mappings without dispatching replication or outbox writes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *configPath == "" || *id == "" || flags.NArg() != 0 {
		return errors.New("--storage-config and --replication-id are required")
	}
	if _, err := os.Stat(*configPath); err != nil {
		return fmt.Errorf("storage configuration: %w", err)
	}
	var buckets []storage.BucketName
	if *selection != "" {
		for _, name := range strings.Split(*selection, ",") {
			bucket, err := storage.NewBucketName(strings.TrimSpace(name))
			if err != nil {
				return err
			}
			buckets = append(buckets, bucket)
		}
	}
	dbs, store := loadStorageConfiguration(*configPath)
	defer func() {
		for _, db := range dbs.Dbs() {
			resultErr = errors.Join(resultErr, db.Close())
		}
	}()
	ctx = lifecycle.WithMaintenance(ctx, *dryRun)
	if err := store.Start(ctx); err != nil {
		return err
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, store.Stop(stopCtx))
	}()
	return replication.ReconcileStorage(ctx, store, *id, buckets, *dryRun)
}
