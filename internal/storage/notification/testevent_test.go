package notification

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type recordingPublisher struct {
	mu        sync.Mutex
	published []*OutboxEntry
	failAll   bool
}

func (p *recordingPublisher) Publish(ctx context.Context, entry *OutboxEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failAll {
		return errors.New("simulated publish failure")
	}
	clone := *entry
	p.published = append(p.published, &clone)
	return nil
}

func (p *recordingPublisher) Validate(ctx context.Context, arn string, destination Destination) error {
	return nil
}

func (p *recordingPublisher) snapshot() []*OutboxEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*OutboxEntry(nil), p.published...)
}

func notifyingStackWithPublisher(t *testing.T, publisher Publisher) *StorageMiddleware {
	t.Helper()
	db := openTestDB(t)
	inner := newSharedDBMetadataStorage(t, db)
	mw, err := NewStorageMiddleware(inner, db, NewSQLRepository(), publisher, "default", time.Minute, DispatcherConfig{}, nil)
	require.NoError(t, err)
	return mw
}

func TestPutBucketNotificationPublishesTestEventPerDestination(t *testing.T) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	mw := notifyingStackWithPublisher(t, publisher)

	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, mw.CreateBucket(ctx, bucket))

	config := &storage.BucketNotificationConfiguration{
		QueueConfigurations: []storage.NotificationConfigurationRule{{
			DestinationType: storage.NotificationDestinationQueue,
			DestinationARN:  "arn:aws:sqs:eu-central-1:000000000000:queue",
			Events:          []string{"s3:ObjectCreated:*"},
		}},
		TopicConfigurations: []storage.NotificationConfigurationRule{{
			DestinationType: storage.NotificationDestinationTopic,
			DestinationARN:  "arn:aws:sns:eu-central-1:000000000000:topic",
			Events:          []string{"s3:ObjectCreated:*"},
		}},
		EventBridgeEnabled: true,
	}
	require.NoError(t, mw.PutBucketNotificationConfiguration(ctx, bucket, config))

	published := publisher.snapshot()
	require.Len(t, published, 3)
	arns := map[string]PayloadFormat{}
	for _, entry := range published {
		require.Equal(t, EventTestEvent, entry.EventName)
		arns[entry.DestinationARN] = entry.PayloadFormat
	}
	require.Contains(t, arns, "arn:aws:sqs:eu-central-1:000000000000:queue")
	require.Contains(t, arns, "arn:aws:sns:eu-central-1:000000000000:topic")
	require.Equal(t, PayloadFormatEventBridge, arns[eventBridgeARNPrefix+"bucket"])

	// Test events must never be written to the durable outbox.
	require.Equal(t, 0, outboxCount(t, mw))
}

func TestPutBucketNotificationSkipsTestEventsWhenValidationSkipped(t *testing.T) {
	ctx := storage.WithSkipNotificationDestinationValidation(context.Background())
	publisher := &recordingPublisher{}
	mw := notifyingStackWithPublisher(t, publisher)

	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, mw.CreateBucket(context.Background(), bucket))

	config := &storage.BucketNotificationConfiguration{
		QueueConfigurations: []storage.NotificationConfigurationRule{{
			DestinationType: storage.NotificationDestinationQueue,
			DestinationARN:  "arn:aws:sqs:eu-central-1:000000000000:queue",
			Events:          []string{"s3:ObjectCreated:*"},
		}},
	}
	require.NoError(t, mw.PutBucketNotificationConfiguration(ctx, bucket, config))
	require.Empty(t, publisher.snapshot(), "no test events when destination validation is skipped")
}

func TestPutBucketNotificationLeavesConfigUnchangedOnTestEventFailure(t *testing.T) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	mw := notifyingStackWithPublisher(t, publisher)

	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, mw.CreateBucket(ctx, bucket))

	original := &storage.BucketNotificationConfiguration{
		QueueConfigurations: []storage.NotificationConfigurationRule{{
			DestinationType: storage.NotificationDestinationQueue,
			DestinationARN:  "arn:aws:sqs:eu-central-1:000000000000:original",
			Events:          []string{"s3:ObjectCreated:*"},
		}},
	}
	require.NoError(t, mw.PutBucketNotificationConfiguration(ctx, bucket, original))

	// A failing test delivery must fail the PUT and leave the stored config intact.
	publisher.failAll = true
	replacement := &storage.BucketNotificationConfiguration{
		QueueConfigurations: []storage.NotificationConfigurationRule{{
			DestinationType: storage.NotificationDestinationQueue,
			DestinationARN:  "arn:aws:sqs:eu-central-1:000000000000:replacement",
			Events:          []string{"s3:ObjectCreated:*"},
		}},
	}
	err := mw.PutBucketNotificationConfiguration(ctx, bucket, replacement)
	require.Error(t, err)

	stored, err := mw.GetBucketNotificationConfiguration(ctx, bucket)
	require.NoError(t, err)
	require.Len(t, stored.QueueConfigurations, 1)
	require.Equal(t, "arn:aws:sqs:eu-central-1:000000000000:original", stored.QueueConfigurations[0].DestinationARN)
}
