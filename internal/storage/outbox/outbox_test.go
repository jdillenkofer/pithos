package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataPartStorageWithOutbox(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		slog.Error(fmt.Sprintf("Couldn't open database: %v", err))
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()

	partStore, err := filesystemPartStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlPartStore: %s", err))
		os.Exit(1)
	}

	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BucketRepository: %s", err))
		os.Exit(1)
	}
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ObjectRepository: %s", err))
		os.Exit(1)
	}
	partRepository, err := repositoryFactory.NewPartRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartRepository: %s", err))
		os.Exit(1)
	}
	tagRepository, err := repositoryFactory.NewTagRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create TagRepository: %s", err))
		os.Exit(1)
	}
	userMetadataRepository, err := repositoryFactory.NewUserMetadataRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create UserMetadataRepository: %s", err))
		os.Exit(1)
	}
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlMetadataStore: %s", err))
		os.Exit(1)
	}

	metadataPartStorage, err := metadatapart.NewStorage(db, metadataStore, partStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlPartStore: %s", err))
		os.Exit(1)
	}

	storagePath2, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath2 := filepath.Join(storagePath2, "pithos.db")
	db2, err := sqlite.OpenDatabase(dbPath2)
	if err != nil {
		slog.Error(fmt.Sprintf("Couldn't open database 2: %v", err))
		os.Exit(1)
	}
	defer func() {
		err = db2.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath2)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath2, err))
			os.Exit(1)
		}
	}()

	// We need a second db, because only one transaction can run at the same time
	// each storage operation opens a new transaction and since outboxStorage and
	// metadataPartStorage would open separate transactions, we would deadlock the test.
	// To avoid this each storage type gets its own db.
	// In the future i want to redesign storage implementations to use the already open transaction.
	storageOutboxEntryRepository, err := repositoryFactory.NewStorageOutboxEntryRepository(db2)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create StorageOutboxEntryRepository: %s", err))
		os.Exit(1)

	}
	reg := prometheus.NewRegistry()
	outboxStorage, err := NewStorage(db2, "default", metadataPartStorage, storageOutboxEntryRepository, reg, 30*time.Second)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create OutboxStorage: %s", err))
		os.Exit(1)
	}

	content := []byte("OutboxStorage")
	err = storage.Tester(outboxStorage, []storage.BucketName{storage.MustNewBucketName("bucket")}, content)
	assert.Nil(t, err)
}

// TestOutboxedPutObjectPreservesPutOptionsOnReplay verifies that tags, object
// metadata and the storage class of a PutObject are persisted with the outbox
// entry and applied when the entry is replayed, instead of forcing such puts
// through the synchronous write-through path.
func TestOutboxedPutObjectPreservesPutOptionsOnReplay(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()

	storagePath := t.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(storagePath, "pithos.db"))
	require.NoError(t, err)
	defer db.Close()

	partStore, err := filesystemPartStore.New(storagePath)
	require.NoError(t, err)
	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	require.NoError(t, err)
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	require.NoError(t, err)
	partRepository, err := repositoryFactory.NewPartRepository(db)
	require.NoError(t, err)
	tagRepository, err := repositoryFactory.NewTagRepository(db)
	require.NoError(t, err)
	userMetadataRepository, err := repositoryFactory.NewUserMetadataRepository(db)
	require.NoError(t, err)
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	require.NoError(t, err)
	metadataPartStorage, err := metadatapart.NewStorage(db, metadataStore, partStore)
	require.NoError(t, err)
	require.NoError(t, metadataPartStorage.Start(ctx))
	defer metadataPartStorage.Stop(ctx)

	// See TestMetadataPartStorageWithOutbox for why the outbox needs its own db.
	storagePath2 := t.TempDir()
	db2, err := sqlite.OpenDatabase(filepath.Join(storagePath2, "pithos.db"))
	require.NoError(t, err)
	defer db2.Close()
	storageOutboxEntryRepository, err := repositoryFactory.NewStorageOutboxEntryRepository(db2)
	require.NoError(t, err)
	outboxStg, err := NewStorage(db2, "default", metadataPartStorage, storageOutboxEntryRepository, prometheus.NewRegistry(), 30*time.Second)
	require.NoError(t, err)
	// The outbox worker is deliberately not started so the queued entries stay
	// put until the test drains them explicitly.
	obs := outboxStg.(*outboxStorage)

	bucketName := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("object-with-options")
	require.NoError(t, outboxStg.CreateBucket(ctx, bucketName))

	contentType := "text/plain"
	storageClass := "STANDARD_IA"
	cacheControl := "max-age=60"
	putResult, err := outboxStg.PutObject(ctx, bucketName, key, &contentType, bytes.NewReader([]byte("content")), nil, &storage.PutObjectOptions{
		Tags:         map[string]string{"env": "test"},
		Metadata:     &storage.ObjectMetadata{CacheControl: &cacheControl, UserMetadata: map[string]string{"owner": "outbox"}},
		StorageClass: &storageClass,
	})
	require.NoError(t, err)
	require.NotNil(t, putResult)

	// Both operations must still be queued: the put must not have been forced
	// through the synchronous write-through path by its options.
	require.NoError(t, database.WithTx(ctx, db2, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		count, err := storageOutboxEntryRepository.Count(ctx, tx.SqlTx(), "default")
		require.NoError(t, err)
		require.Equal(t, 2, count)
		return nil
	}))

	obs.maybeProcessOutboxEntries(ctx)

	object, err := metadataPartStorage.HeadObject(ctx, bucketName, key, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"env": "test"}, object.Tags)
	require.NotNil(t, object.Metadata.CacheControl)
	require.Equal(t, cacheControl, *object.Metadata.CacheControl)
	require.Equal(t, map[string]string{"owner": "outbox"}, object.Metadata.UserMetadata)
	require.Equal(t, storageClass, metadatastore.EffectiveStorageClass(object.StorageClass))
}
