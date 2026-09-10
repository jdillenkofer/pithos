package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestReconcileReplicationCLI(t *testing.T) {
	ctx := lifecycle.WithMaintenance(t.Context(), false)
	dir := t.TempDir()
	config := func(name string) map[string]any {
		ref := map[string]any{"type": "DatabaseReference", "refName": name}
		return map[string]any{"type": "MetadataPartStorage", "db": map[string]any{"type": "RegisterDatabaseReference", "refName": name, "db": map[string]any{"type": "SqliteDatabase", "dbPath": filepath.Join(dir, name+".sqlite")}}, "metadataStore": map[string]any{"type": "SqlMetadataStore", "db": ref}, "partStore": map[string]any{"type": "SqlPartStore", "db": ref}}
	}
	write := func(name string, value any) string {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		path := filepath.Join(dir, name+".json")
		require.NoError(t, os.WriteFile(path, data, 0600))
		return path
	}
	primary, secondary := config("primary"), config("secondary")
	primaryPath, secondaryPath := write("primary", primary), write("secondary", secondary)
	topology := write("replication", map[string]any{"type": "ReplicationStorage", "replicationId": "cli", "secondaryIds": []string{"secondary"}, "primaryStorage": primary, "secondaryStorages": []any{secondary}})
	dbs, source := loadStorageConfiguration(primaryPath, prometheus.NewRegistry())
	require.NoError(t, source.Start(ctx))
	bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
	require.NoError(t, source.CreateBucket(ctx, bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
	_, err := source.PutObject(ctx, bucket, key, nil, strings.NewReader("old version"), nil, nil)
	require.NoError(t, err)
	require.NoError(t, source.Stop(ctx))
	for _, db := range dbs.Dbs() {
		require.NoError(t, db.Close())
	}
	args := []string{"--storage-config", topology, "--replication-id", "cli", "--bucket", "bucket"}
	require.NoError(t, reconcileReplication(t.Context(), append(args, "--dry-run")))
	dbs, target := loadStorageConfiguration(secondaryPath, prometheus.NewRegistry())
	require.NoError(t, target.Start(ctx))
	_, err = target.HeadBucket(ctx, bucket)
	require.ErrorIs(t, err, storage.ErrNoSuchBucket)
	require.NoError(t, target.Stop(ctx))
	for _, db := range dbs.Dbs() {
		require.NoError(t, db.Close())
	}
	require.NoError(t, reconcileReplication(t.Context(), args))
	// Running the command again resumes from durable mappings without duplicates.
	require.NoError(t, reconcileReplication(t.Context(), args))
	dbs, target = loadStorageConfiguration(secondaryPath, prometheus.NewRegistry())
	require.NoError(t, target.Start(ctx))
	versions, err := target.ListObjectVersions(ctx, bucket, storage.ListObjectVersionsOptions{MaxKeys: 100})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1)
	require.NoError(t, target.Stop(ctx))
	for _, db := range dbs.Dbs() {
		require.NoError(t, db.Close())
	}
}
