package notification

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type StorageMiddleware struct {
	delegator.DelegatingStorage
	*lifecycle.ValidatedLifecycle
	db                 database.Database
	repository         Repository
	publisher          Publisher
	outboxID           string
	claimOwner         string
	claimLeaseDuration time.Duration
	trigger            chan struct{}
	taskHandle         *task.TaskHandle
	tracer             trace.Tracer
}

var _ storage.Storage = (*StorageMiddleware)(nil)
var _ storage.TransactionalStorage = (*StorageMiddleware)(nil)

func NewStorageMiddleware(inner storage.Storage, db database.Database, repository Repository, publisher Publisher, outboxID string, claimLeaseDuration time.Duration) (*StorageMiddleware, error) {
	validatedLifecycle, err := lifecycle.NewValidatedLifecycle("NotificationStorageMiddleware")
	if err != nil {
		return nil, err
	}
	if claimLeaseDuration <= 0 {
		claimLeaseDuration = 30 * time.Second
	}
	if outboxID == "" {
		outboxID = "default"
	}
	return &StorageMiddleware{
		DelegatingStorage:  delegator.Wrap(inner),
		ValidatedLifecycle: validatedLifecycle,
		db:                 db,
		repository:         repository,
		publisher:          publisher,
		outboxID:           outboxID,
		claimOwner:         outboxID + ":notification:" + ulid.Make().String(),
		claimLeaseDuration: claimLeaseDuration,
		trigger:            make(chan struct{}, 16),
		tracer:             otel.Tracer("internal/storage/notification"),
	}, nil
}

func (m *StorageMiddleware) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return delegator.WithTransaction(ctx, opts, m.Next, m, fn)
}

func (m *StorageMiddleware) Start(ctx context.Context) error {
	if err := m.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	m.taskHandle = task.Start(func(_ *atomic.Bool) {
		m.dispatchLoop()
	})
	return m.Next.Start(ctx)
}

func (m *StorageMiddleware) Stop(ctx context.Context) error {
	if err := m.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	close(m.trigger)
	if m.taskHandle != nil {
		m.taskHandle.JoinWithTimeout(30 * time.Second)
	}
	return m.Next.Stop(ctx)
}

func (m *StorageMiddleware) PutBucketNotificationConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketNotificationConfiguration) error {
	if !storage.SkipNotificationDestinationValidation(ctx) {
		if err := m.validateConfiguredDestinations(ctx, config); err != nil {
			return err
		}
	}
	return m.Next.PutBucketNotificationConfiguration(ctx, bucketName, config)
}

func (m *StorageMiddleware) PutObject(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, contentType *string, dataReader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	result, err := m.Next.PutObject(ctx, bucket, key, contentType, dataReader, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	event := ObjectEvent{EventName: EventObjectCreatedPut, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
	if result != nil {
		event.VersionID = result.VersionID
		event.ETag = result.ETag
	}
	m.fillObjectDetails(ctx, &event)
	m.enqueueMatching(ctx, event)
	return result, nil
}

func (m *StorageMiddleware) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	result, err := m.Next.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, opts)
	if err != nil {
		return nil, err
	}
	event := ObjectEvent{EventName: EventObjectCreatedCopy, Bucket: dstBucket, Key: dstKey, EventTime: time.Now().UTC()}
	if result != nil {
		event.VersionID = result.VersionID
		event.ETag = &result.ETag
	}
	m.fillObjectDetails(ctx, &event)
	m.enqueueMatching(ctx, event)
	return result, nil
}

func (m *StorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, uploadID storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	result, err := m.Next.CompleteMultipartUpload(ctx, bucket, key, uploadID, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	event := ObjectEvent{EventName: EventObjectCreatedCompleteMultipartUpload, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
	if result != nil {
		event.VersionID = result.VersionID
		event.ETag = &result.ETag
	}
	m.fillObjectDetails(ctx, &event)
	m.enqueueMatching(ctx, event)
	return result, nil
}

func (m *StorageMiddleware) DeleteObject(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	result, err := m.Next.DeleteObject(ctx, bucket, key, opts)
	if err != nil {
		return nil, err
	}
	eventName := EventObjectRemovedDelete
	if result != nil && result.IsDeleteMarker {
		eventName = EventObjectRemovedDeleteMarkerCreated
	}
	event := ObjectEvent{EventName: eventName, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
	if result != nil {
		event.VersionID = result.VersionID
	}
	m.enqueueMatching(ctx, event)
	return result, nil
}

func (m *StorageMiddleware) DeleteObjects(ctx context.Context, bucket storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	result, err := m.Next.DeleteObjects(ctx, bucket, entries)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return result, nil
	}
	for _, deleted := range result.Entries {
		if !deleted.Deleted {
			continue
		}
		eventName := EventObjectRemovedDelete
		if deleted.DeleteMarker != nil && *deleted.DeleteMarker {
			eventName = EventObjectRemovedDeleteMarkerCreated
		}
		event := ObjectEvent{EventName: eventName, Bucket: bucket, Key: deleted.Key, EventTime: time.Now().UTC()}
		if deleted.DeleteMarkerVersionID != nil {
			event.VersionID = deleted.DeleteMarkerVersionID
		} else {
			event.VersionID = deleted.VersionID
		}
		m.enqueueMatching(ctx, event)
	}
	return result, nil
}

func (m *StorageMiddleware) PutObjectTagging(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, tags map[string]string, opts *storage.ObjectTaggingOptions) error {
	err := m.Next.PutObjectTagging(ctx, bucket, key, tags, opts)
	if err != nil {
		return err
	}
	event := ObjectEvent{EventName: EventObjectTaggingPut, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
	if opts != nil {
		event.VersionID = opts.VersionID
	}
	m.fillObjectDetails(ctx, &event)
	m.enqueueMatching(ctx, event)
	return nil
}

func (m *StorageMiddleware) DeleteObjectTagging(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) error {
	err := m.Next.DeleteObjectTagging(ctx, bucket, key, opts)
	if err != nil {
		return err
	}
	event := ObjectEvent{EventName: EventObjectTaggingDelete, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
	if opts != nil {
		event.VersionID = opts.VersionID
	}
	m.fillObjectDetails(ctx, &event)
	m.enqueueMatching(ctx, event)
	return nil
}

func (m *StorageMiddleware) TransitionObjectStorageClass(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, targetStorageClass string, opts *storage.TransitionObjectStorageClassOptions) error {
	if err := m.Next.TransitionObjectStorageClass(ctx, bucket, key, targetStorageClass, opts); err != nil {
		return err
	}
	event := ObjectEvent{EventName: EventLifecycleTransition, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
	if opts != nil {
		event.VersionID = opts.VersionID
	}
	m.fillObjectDetails(ctx, &event)
	m.enqueueMatching(ctx, event)
	return nil
}

func (m *StorageMiddleware) fillObjectDetails(ctx context.Context, event *ObjectEvent) {
	opts := (*storage.HeadObjectOptions)(nil)
	if event.VersionID != nil {
		opts = &storage.HeadObjectOptions{VersionID: event.VersionID}
	}
	object, err := m.Next.HeadObject(ctx, event.Bucket, event.Key, opts)
	if err != nil {
		slog.DebugContext(ctx, "Failed to read object details for notification", "bucket", event.Bucket.String(), "key", event.Key.String(), "error", err)
		return
	}
	event.Size = &object.Size
	event.ETag = &object.ETag
	if object.VersionID != nil {
		event.VersionID = object.VersionID
	}
}

func (m *StorageMiddleware) validateConfiguredDestinations(ctx context.Context, config *storage.BucketNotificationConfiguration) error {
	if config == nil {
		return nil
	}
	for _, rule := range allRules(config) {
		destination, ok := destinationFor(m.publisher, rule.DestinationARN)
		if !ok {
			continue
		}
		if err := m.publisher.Validate(ctx, rule.DestinationARN, destination); err != nil {
			return err
		}
	}
	return nil
}

func destinationFor(p Publisher, arn string) (Destination, bool) {
	registry, ok := p.(*RegistryPublisher)
	if !ok {
		return Destination{}, false
	}
	destination, ok := registry.Destinations[arn]
	return destination, ok
}

func (m *StorageMiddleware) enqueueMatching(ctx context.Context, event ObjectEvent) {
	config, err := m.Next.GetBucketNotificationConfiguration(ctx, event.Bucket)
	if err != nil {
		slog.WarnContext(ctx, "Failed to read bucket notification configuration", "bucket", event.Bucket.String(), "error", err)
		return
	}
	var entries []OutboxEntry
	for _, rule := range allRules(config) {
		if !RuleMatches(rule, event) {
			continue
		}
		payloadFormat := payloadFormatForDestination(m.publisher, rule.DestinationARN, PayloadFormatS3Records)
		payload, err := buildPayload(payloadFormat, event)
		if err != nil {
			slog.WarnContext(ctx, "Failed to build notification payload", "payloadFormat", payloadFormat, "error", err)
			continue
		}
		entries = append(entries, OutboxEntry{DestinationARN: rule.DestinationARN, EventName: event.EventName, PayloadFormat: payloadFormat, Payload: payload})
	}
	eventBridgeDestinationARN := "eventbridge:" + event.Bucket.String()
	if config.EventBridgeEnabled && publisherHasDestination(m.publisher, eventBridgeDestinationARN) {
		payloadFormat := payloadFormatForDestination(m.publisher, eventBridgeDestinationARN, PayloadFormatEventBridge)
		payload, err := buildPayload(payloadFormat, event)
		if err == nil {
			entries = append(entries, OutboxEntry{DestinationARN: eventBridgeDestinationARN, EventName: event.EventName, PayloadFormat: payloadFormat, Payload: payload})
		}
	}
	if len(entries) == 0 {
		return
	}
	err = database.WithTx(ctx, m.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		for i := range entries {
			if err := m.repository.Save(ctx, tx.SqlTx(), m.outboxID, &entries[i]); err != nil {
				return err
			}
		}
		tx.OnAfterCommit(func(context.Context) error {
			select {
			case m.trigger <- struct{}{}:
			default:
			}
			return nil
		})
		return nil
	})
	if err != nil {
		slog.WarnContext(ctx, "Failed to enqueue notification entries", "error", err)
	}
}

func publisherHasDestination(p Publisher, arn string) bool {
	_, ok := destinationFor(p, arn)
	return ok
}

func payloadFormatForDestination(p Publisher, arn string, defaultFormat PayloadFormat) PayloadFormat {
	destination, ok := destinationFor(p, arn)
	if !ok || destination.PayloadFormat == "" {
		return defaultFormat
	}
	return destination.PayloadFormat
}

func buildPayload(payloadFormat PayloadFormat, event ObjectEvent) ([]byte, error) {
	switch payloadFormat {
	case "", PayloadFormatS3Records:
		return BuildS3RecordsPayload(event)
	case PayloadFormatEventBridge:
		return BuildEventBridgePayload(event)
	default:
		return nil, fmt.Errorf("unsupported notification payload format %q", payloadFormat)
	}
}

func allRules(config *storage.BucketNotificationConfiguration) []storage.NotificationConfigurationRule {
	if config == nil {
		return nil
	}
	rules := make([]storage.NotificationConfigurationRule, 0, len(config.TopicConfigurations)+len(config.QueueConfigurations)+len(config.CloudFunctionConfigurations))
	rules = append(rules, config.TopicConfigurations...)
	rules = append(rules, config.QueueConfigurations...)
	rules = append(rules, config.CloudFunctionConfigurations...)
	return rules
}

func (m *StorageMiddleware) dispatchLoop() {
	ctx := context.Background()
	for {
		select {
		case _, ok := <-m.trigger:
			if !ok {
				return
			}
		case <-time.After(time.Second):
		}
		m.dispatchAvailable(ctx)
	}
}

func (m *StorageMiddleware) dispatchAvailable(ctx context.Context) {
	for {
		entry, claimed, err := m.claim(ctx)
		if err != nil || entry == nil || !claimed {
			if err != nil {
				slog.WarnContext(ctx, "Failed to claim notification entry", "error", err)
			}
			return
		}
		err = m.publisher.Publish(ctx, entry)
		if err == nil {
			_ = m.deleteClaimed(ctx, entry)
			continue
		}
		slog.WarnContext(ctx, "Failed to publish notification", "destinationARN", entry.DestinationARN, "attempts", entry.Attempts, "error", err)
		_ = m.release(ctx, entry)
		return
	}
}

func (m *StorageMiddleware) claim(ctx context.Context) (*OutboxEntry, bool, error) {
	var entry *OutboxEntry
	var claimed bool
	now := time.Now().UTC()
	err := database.WithTx(ctx, m.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var err error
		entry, claimed, err = m.repository.ClaimFirst(ctx, tx.SqlTx(), m.outboxID, m.claimOwner, now, now.Add(m.claimLeaseDuration))
		return err
	})
	return entry, claimed, err
}

func (m *StorageMiddleware) deleteClaimed(ctx context.Context, entry *OutboxEntry) error {
	return database.WithTx(ctx, m.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		_, err := m.repository.DeleteByClaimOwner(ctx, tx.SqlTx(), m.outboxID, *entry.ID, m.claimOwner)
		return err
	})
}

func (m *StorageMiddleware) release(ctx context.Context, entry *OutboxEntry) error {
	delaySeconds := math.Min(300, math.Pow(2, float64(entry.Attempts)))
	nextAttemptAt := time.Now().UTC().Add(time.Duration(delaySeconds) * time.Second)
	return database.WithTx(ctx, m.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		_, err := m.repository.ReleaseClaim(ctx, tx.SqlTx(), m.outboxID, *entry.ID, m.claimOwner, nextAttemptAt, time.Now().UTC())
		return err
	})
}
