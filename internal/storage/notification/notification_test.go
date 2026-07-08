package notification

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/stretchr/testify/require"
)

func TestRuleMatchesWildcardAndFilters(t *testing.T) {
	rule := storage.NotificationConfigurationRule{
		Events: []string{"s3:ObjectCreated:*"},
		FilterRules: []storage.NotificationFilterRule{
			{Name: "prefix", Value: "images/"},
			{Name: "suffix", Value: ".jpg"},
		},
	}

	require.True(t, RuleMatches(rule, ObjectEvent{
		EventName: EventObjectCreatedPut,
		Bucket:    storage.MustNewBucketName("bucket"),
		Key:       storage.MustNewObjectKey("images/a.jpg"),
	}))
	require.False(t, RuleMatches(rule, ObjectEvent{
		EventName: EventObjectRemovedDelete,
		Bucket:    storage.MustNewBucketName("bucket"),
		Key:       storage.MustNewObjectKey("images/a.jpg"),
	}))
	require.False(t, RuleMatches(rule, ObjectEvent{
		EventName: EventObjectCreatedPut,
		Bucket:    storage.MustNewBucketName("bucket"),
		Key:       storage.MustNewObjectKey("images/a.png"),
	}))
}

func TestBuildS3RecordsPayload(t *testing.T) {
	etag := `"abc"`
	size := int64(42)
	payload, err := BuildS3RecordsPayload(ObjectEvent{
		EventName: EventObjectCreatedPut,
		Bucket:    storage.MustNewBucketName("bucket"),
		Key:       storage.MustNewObjectKey("key"),
		ETag:      &etag,
		Size:      &size,
		EventTime: time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	var decoded map[string][]map[string]any
	require.NoError(t, json.Unmarshal(payload, &decoded))
	require.Equal(t, "ObjectCreated:Put", decoded["Records"][0]["eventName"])
}

func TestSQLRepositoryClaimReleaseAndDelete(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.OpenDatabase(":memory:")
	require.NoError(t, err)
	defer db.Close()

	repo := NewSQLRepository()
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return repo.Save(ctx, tx.SqlTx(), "one", &OutboxEntry{
			DestinationARN: "arn:aws:sns:eu-central-1:000000000000:topic",
			EventName:      EventObjectCreatedPut,
			PayloadFormat:  PayloadFormatS3Records,
			Payload:        []byte(`{"Records":[]}`),
		})
	}))

	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		now := time.Now().UTC()
		other, ok, err := repo.ClaimFirst(ctx, tx.SqlTx(), "other", "owner", now, now.Add(time.Minute))
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, other)
		return nil
	}))

	var claimed *OutboxEntry
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var err error
		var ok bool
		now := time.Now().UTC()
		claimed, ok, err = repo.ClaimFirst(ctx, tx.SqlTx(), "one", "owner", now, now.Add(time.Minute))
		require.True(t, ok)
		return err
	}))
	require.NotNil(t, claimed)
	require.Equal(t, 1, claimed.Attempts)

	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		ok, err := repo.ReleaseClaim(ctx, tx.SqlTx(), "one", *claimed.ID, "owner", time.Now().UTC(), time.Now().UTC())
		require.True(t, ok)
		return err
	}))
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var err error
		var ok bool
		now := time.Now().UTC()
		claimed, ok, err = repo.ClaimFirst(ctx, tx.SqlTx(), "one", "owner", now, now.Add(time.Minute))
		require.True(t, ok)
		return err
	}))
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		ok, err := repo.DeleteByClaimOwner(ctx, tx.SqlTx(), "one", *claimed.ID, "owner")
		require.True(t, ok)
		return err
	}))
}
