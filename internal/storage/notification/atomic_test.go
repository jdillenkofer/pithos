package notification

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	sqlPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

// failingSaveRepository wraps a Repository and fails the outbox insert on demand
// so tests can prove that a failed enqueue rolls back the preceding mutation.
type failingSaveRepository struct {
	Repository
	failSave bool
}

func (r *failingSaveRepository) Save(ctx context.Context, tx *sql.Tx, outboxID string, entry *OutboxEntry) error {
	if r.failSave {
		return errors.New("simulated outbox insert failure")
	}
	return r.Repository.Save(ctx, tx, outboxID, entry)
}

func newSharedDBMetadataStorage(t *testing.T, db database.Database) storage.Storage {
	t.Helper()
	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	require.NoError(t, err)
	partStore, err := sqlPartStore.New(db, partContentRepository)
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
	inner, err := metadatapart.NewStorage(db, metadataStore, partStore)
	require.NoError(t, err)
	return inner
}

// notifyingStackWithFailToggle builds a notification middleware over a metadata
// storage that shares the same database, so enqueue and mutation are atomic. The
// returned toggle switches the outbox insert into failure mode.
func openTestDB(t *testing.T) database.Database {
	t.Helper()
	testutils.SkipIfIntegration(t)
	dbPath := filepath.Join(t.TempDir(), "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func notifyingStackWithFailToggle(t *testing.T) (*StorageMiddleware, *failingSaveRepository) {
	t.Helper()
	db := openTestDB(t)
	inner := newSharedDBMetadataStorage(t, db)
	repo := &failingSaveRepository{Repository: NewSQLRepository()}
	// A recording publisher keeps the synchronous test-event delivery during
	// PutBucketNotificationConfiguration local instead of reaching real AWS.
	mw, err := NewStorageMiddleware(inner, db, repo, &recordingPublisher{}, "default", time.Minute)
	require.NoError(t, err)
	return mw, repo
}

func matchingQueueConfig() *storage.BucketNotificationConfiguration {
	return &storage.BucketNotificationConfiguration{
		QueueConfigurations: []storage.NotificationConfigurationRule{{
			DestinationType: storage.NotificationDestinationQueue,
			DestinationARN:  "arn:aws:sqs:eu-central-1:000000000000:queue",
			Events:          []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*", "s3:ObjectTagging:*"},
		}},
	}
}

func TestPutObjectRollsBackWhenOutboxInsertFails(t *testing.T) {
	ctx := context.Background()
	mw, repo := notifyingStackWithFailToggle(t)

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("key")
	require.NoError(t, mw.CreateBucket(ctx, bucket))
	require.NoError(t, mw.PutBucketNotificationConfiguration(ctx, bucket, matchingQueueConfig()))

	// Sanity: the mutation succeeds and enqueues while the outbox is healthy.
	_, err := mw.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, outboxCount(t, mw))

	// With the outbox insert failing, the mutation must roll back entirely.
	repo.failSave = true
	failingKey := storage.MustNewObjectKey("failing-key")
	_, err = mw.PutObject(ctx, bucket, failingKey, nil, bytes.NewReader([]byte("world")), nil, nil)
	require.Error(t, err)

	_, err = mw.HeadObject(ctx, bucket, failingKey, nil)
	require.Error(t, err, "object must not exist after a rolled-back PutObject")
	require.Equal(t, 1, outboxCount(t, mw), "no new outbox entry must be committed")
}

func TestDeleteObjectRollsBackWhenOutboxInsertFails(t *testing.T) {
	ctx := context.Background()
	mw, repo := notifyingStackWithFailToggle(t)

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("key")
	require.NoError(t, mw.CreateBucket(ctx, bucket))
	require.NoError(t, mw.PutBucketNotificationConfiguration(ctx, bucket, matchingQueueConfig()))
	_, err := mw.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	repo.failSave = true
	_, err = mw.DeleteObject(ctx, bucket, key, nil)
	require.Error(t, err)

	// The object must still be readable because the delete rolled back.
	_, err = mw.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err, "object must survive a rolled-back DeleteObject")
}

func TestPutObjectTaggingRollsBackWhenOutboxInsertFails(t *testing.T) {
	ctx := context.Background()
	mw, repo := notifyingStackWithFailToggle(t)

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("key")
	require.NoError(t, mw.CreateBucket(ctx, bucket))
	require.NoError(t, mw.PutBucketNotificationConfiguration(ctx, bucket, matchingQueueConfig()))
	_, err := mw.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	repo.failSave = true
	err = mw.PutObjectTagging(ctx, bucket, key, map[string]string{"env": "prod"}, nil)
	require.Error(t, err)

	tags, err := mw.GetObjectTagging(ctx, bucket, key, nil)
	require.NoError(t, err)
	require.Empty(t, tags, "tags must not be persisted after a rolled-back PutObjectTagging")
}

func outboxCount(t *testing.T, mw *StorageMiddleware) int {
	t.Helper()
	var count int
	require.NoError(t, database.WithTx(context.Background(), mw.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		count, err = mw.repository.Count(ctx, tx.SqlTx(), mw.outboxID)
		return err
	}))
	return count
}
