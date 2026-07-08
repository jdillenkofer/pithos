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

type databaseBackedStorage interface {
	Database() database.Database
}

type databaseWrapper interface {
	UnwrapDatabase() database.Database
}

func NewStorageMiddleware(inner storage.Storage, db database.Database, repository Repository, publisher Publisher, outboxID string, claimLeaseDuration time.Duration) (*StorageMiddleware, error) {
	validatedLifecycle, err := lifecycle.NewValidatedLifecycle("NotificationStorageMiddleware")
	if err != nil {
		return nil, err
	}
	if dbStorage, ok := inner.(databaseBackedStorage); ok {
		innerDB := dbStorage.Database()
		if sameDatabaseHandle(innerDB, db) {
			db = innerDB
		}
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

func sameDatabaseHandle(a database.Database, b database.Database) bool {
	if a == b {
		return true
	}
	if wrapped, ok := a.(databaseWrapper); ok && wrapped.UnwrapDatabase() == b {
		return true
	}
	if wrapped, ok := b.(databaseWrapper); ok && wrapped.UnwrapDatabase() == a {
		return true
	}
	return false
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
	var result *storage.PutObjectResult
	err := m.runWithNotifications(ctx, func(ctx context.Context) ([]ObjectEvent, error) {
		var err error
		result, err = m.Next.PutObject(ctx, bucket, key, contentType, dataReader, checksumInput, opts)
		if err != nil {
			return nil, err
		}
		event := ObjectEvent{EventName: EventObjectCreatedPut, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
		if result != nil {
			event.VersionID = result.VersionID
			event.ETag = result.ETag
		}
		m.fillObjectDetails(ctx, &event)
		return []ObjectEvent{event}, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *StorageMiddleware) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	var result *storage.CopyObjectResult
	err := m.runWithNotifications(ctx, func(ctx context.Context) ([]ObjectEvent, error) {
		var err error
		result, err = m.Next.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, opts)
		if err != nil {
			return nil, err
		}
		event := ObjectEvent{EventName: EventObjectCreatedCopy, Bucket: dstBucket, Key: dstKey, EventTime: time.Now().UTC()}
		if result != nil {
			event.VersionID = result.VersionID
			event.ETag = &result.ETag
		}
		m.fillObjectDetails(ctx, &event)
		return []ObjectEvent{event}, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *StorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, uploadID storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	var result *storage.CompleteMultipartUploadResult
	err := m.runWithNotifications(ctx, func(ctx context.Context) ([]ObjectEvent, error) {
		var err error
		result, err = m.Next.CompleteMultipartUpload(ctx, bucket, key, uploadID, checksumInput, opts)
		if err != nil {
			return nil, err
		}
		event := ObjectEvent{EventName: EventObjectCreatedCompleteMultipartUpload, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
		if result != nil {
			event.VersionID = result.VersionID
			event.ETag = &result.ETag
		}
		m.fillObjectDetails(ctx, &event)
		return []ObjectEvent{event}, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *StorageMiddleware) DeleteObject(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	var result *storage.DeleteObjectResult
	err := m.runWithNotifications(ctx, func(ctx context.Context) ([]ObjectEvent, error) {
		var err error
		result, err = m.Next.DeleteObject(ctx, bucket, key, opts)
		if err != nil {
			return nil, err
		}
		eventName := EventObjectRemovedDelete
		if result != nil && result.IsDeleteMarker {
			eventName = EventObjectRemovedDeleteMarkerCreated
		}
		if overrideEventName, ok := storage.NotificationEventOverride(ctx); ok {
			eventName = overrideEventName
			if overrideEventName == EventLifecycleExpirationDelete && result != nil && result.IsDeleteMarker {
				eventName = EventLifecycleExpirationDeleteMarker
			}
		}
		event := ObjectEvent{EventName: eventName, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
		if result != nil {
			event.VersionID = result.VersionID
		}
		return []ObjectEvent{event}, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *StorageMiddleware) DeleteObjects(ctx context.Context, bucket storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	var result *storage.DeleteObjectsResult
	err := m.runWithNotifications(ctx, func(ctx context.Context) ([]ObjectEvent, error) {
		var err error
		result, err = m.Next.DeleteObjects(ctx, bucket, entries)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, nil
		}
		var events []ObjectEvent
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
			events = append(events, event)
		}
		return events, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *StorageMiddleware) PutObjectTagging(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, tags map[string]string, opts *storage.ObjectTaggingOptions) error {
	return m.runWithNotifications(ctx, func(ctx context.Context) ([]ObjectEvent, error) {
		if err := m.Next.PutObjectTagging(ctx, bucket, key, tags, opts); err != nil {
			return nil, err
		}
		event := ObjectEvent{EventName: EventObjectTaggingPut, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
		if opts != nil {
			event.VersionID = opts.VersionID
		}
		m.fillObjectDetails(ctx, &event)
		return []ObjectEvent{event}, nil
	})
}

func (m *StorageMiddleware) DeleteObjectTagging(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) error {
	return m.runWithNotifications(ctx, func(ctx context.Context) ([]ObjectEvent, error) {
		if err := m.Next.DeleteObjectTagging(ctx, bucket, key, opts); err != nil {
			return nil, err
		}
		event := ObjectEvent{EventName: EventObjectTaggingDelete, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
		if opts != nil {
			event.VersionID = opts.VersionID
		}
		m.fillObjectDetails(ctx, &event)
		return []ObjectEvent{event}, nil
	})
}

func (m *StorageMiddleware) TransitionObjectStorageClass(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, targetStorageClass string, opts *storage.TransitionObjectStorageClassOptions) error {
	return m.runWithNotifications(ctx, func(ctx context.Context) ([]ObjectEvent, error) {
		if err := m.Next.TransitionObjectStorageClass(ctx, bucket, key, targetStorageClass, opts); err != nil {
			return nil, err
		}
		eventName, ok := storage.NotificationEventOverride(ctx)
		if !ok {
			return nil, nil
		}
		event := ObjectEvent{EventName: eventName, Bucket: bucket, Key: key, EventTime: time.Now().UTC()}
		if opts != nil {
			event.VersionID = opts.VersionID
		}
		m.fillObjectDetails(ctx, &event)
		return []ObjectEvent{event}, nil
	})
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

// runWithNotifications performs an object mutation and the outbox enqueue that
// results from it inside a single database transaction. The transaction-bearing
// context is passed to mutate, so when the wrapped storage shares the same
// database.Database the mutation, the notification configuration read, and the
// outbox inserts all commit atomically: a failed outbox insert rolls back the
// mutation. When the wrapped storage uses a different database.Database it opens
// its own independent transaction, so enqueue is only best-effort for that
// mutation (the mutation has already committed before enqueue runs).
func (m *StorageMiddleware) runWithNotifications(ctx context.Context, mutate func(ctx context.Context) ([]ObjectEvent, error)) error {
	return database.WithTx(ctx, m.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		events, err := mutate(ctx)
		if err != nil {
			return err
		}
		return m.enqueueEvents(ctx, tx, events)
	})
}

func (m *StorageMiddleware) enqueueEvents(ctx context.Context, tx database.Tx, events []ObjectEvent) error {
	if len(events) == 0 {
		return nil
	}
	configCache := map[string]*storage.BucketNotificationConfiguration{}
	var entries []OutboxEntry
	for _, event := range events {
		bucketName := event.Bucket.String()
		config, ok := configCache[bucketName]
		if !ok {
			var err error
			config, err = m.Next.GetBucketNotificationConfiguration(ctx, event.Bucket)
			if err != nil {
				return err
			}
			configCache[bucketName] = config
		}
		eventEntries, err := m.buildEntriesForEvent(config, event)
		if err != nil {
			return err
		}
		entries = append(entries, eventEntries...)
	}
	if len(entries) == 0 {
		return nil
	}
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
}

func (m *StorageMiddleware) buildEntriesForEvent(config *storage.BucketNotificationConfiguration, event ObjectEvent) ([]OutboxEntry, error) {
	var entries []OutboxEntry
	for _, rule := range allRules(config) {
		if !RuleMatches(rule, event) {
			continue
		}
		payloadFormat := payloadFormatForDestination(m.publisher, rule.DestinationARN, PayloadFormatS3Records)
		payload, err := buildPayload(payloadFormat, event)
		if err != nil {
			return nil, err
		}
		entries = append(entries, OutboxEntry{DestinationARN: rule.DestinationARN, EventName: event.EventName, PayloadFormat: payloadFormat, Payload: payload})
	}
	if config != nil && config.EventBridgeEnabled {
		// EventBridge-enabled buckets always enqueue an entry. If a registry
		// destination is configured for "eventbridge:<bucket>" it takes
		// precedence; otherwise the entry is delivered to the AWS EventBridge
		// default event bus.
		eventBridgeDestinationARN := eventBridgeARNPrefix + event.Bucket.String()
		payloadFormat := payloadFormatForDestination(m.publisher, eventBridgeDestinationARN, PayloadFormatEventBridge)
		payload, err := buildPayload(payloadFormat, event)
		if err != nil {
			return nil, err
		}
		entries = append(entries, OutboxEntry{DestinationARN: eventBridgeDestinationARN, EventName: event.EventName, PayloadFormat: payloadFormat, Payload: payload})
	}
	return entries, nil
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
