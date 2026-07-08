package notification

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
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
	dispatcher         DispatcherConfig
	metrics            *notificationMetrics
	trigger            chan struct{}
	taskHandle         *task.TaskHandle
	tracer             trace.Tracer
}

// DispatcherConfig tunes the durable delivery loop. Zero values are replaced by
// conservative defaults that preserve the historical single-worker,
// unbounded-retry behavior.
type DispatcherConfig struct {
	// MaxAttempts is the number of delivery attempts after which an entry is
	// dead-lettered and no longer retried. A value <= 0 means unlimited retries.
	MaxAttempts int
	// MinBackoff and MaxBackoff bound the exponential retry backoff.
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// Concurrency is the number of entries dispatched in parallel per batch.
	Concurrency int
	// BatchSize is the maximum number of entries claimed before dispatching.
	BatchSize int
}

func (c DispatcherConfig) withDefaults() DispatcherConfig {
	if c.MinBackoff <= 0 {
		c.MinBackoff = time.Second
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 300 * time.Second
	}
	if c.MaxBackoff < c.MinBackoff {
		c.MaxBackoff = c.MinBackoff
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1
	}
	return c
}

var _ storage.Storage = (*StorageMiddleware)(nil)
var _ storage.TransactionalStorage = (*StorageMiddleware)(nil)

type databaseBackedStorage interface {
	Database() database.Database
}

type databaseWrapper interface {
	UnwrapDatabase() database.Database
}

func NewStorageMiddleware(inner storage.Storage, db database.Database, repository Repository, publisher Publisher, outboxID string, claimLeaseDuration time.Duration, dispatcher DispatcherConfig, registerer prometheus.Registerer) (*StorageMiddleware, error) {
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
		dispatcher:         dispatcher.withDefaults(),
		metrics:            newNotificationMetrics(registerer),
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
		// Deliver a synchronous s3:TestEvent to every configured destination
		// before persisting. A failed test publish fails the request and leaves
		// the previously stored configuration untouched. Test events are never
		// written to the durable outbox.
		if err := m.publishTestEvents(ctx, bucketName, config); err != nil {
			return err
		}
	}
	return m.Next.PutBucketNotificationConfiguration(ctx, bucketName, config)
}

func (m *StorageMiddleware) publishTestEvents(ctx context.Context, bucketName storage.BucketName, config *storage.BucketNotificationConfiguration) error {
	if config == nil {
		return nil
	}
	seen := map[string]struct{}{}
	publish := func(arn string, defaultFormat PayloadFormat) error {
		if _, ok := seen[arn]; ok {
			return nil
		}
		seen[arn] = struct{}{}
		payloadFormat := payloadFormatForDestination(m.publisher, arn, defaultFormat)
		payload, err := BuildTestEventPayload(payloadFormat, bucketName)
		if err != nil {
			return err
		}
		entry := &OutboxEntry{DestinationARN: arn, EventName: EventTestEvent, PayloadFormat: payloadFormat, Payload: payload}
		if err := m.publisher.Publish(ctx, entry); err != nil {
			return fmt.Errorf("test event delivery to destination %q failed: %w", arn, err)
		}
		return nil
	}
	for _, rule := range allRules(config) {
		if err := publish(rule.DestinationARN, PayloadFormatS3Records); err != nil {
			return err
		}
	}
	if config.EventBridgeEnabled {
		if err := publish(eventBridgeARNPrefix+bucketName.String(), PayloadFormatEventBridge); err != nil {
			return err
		}
	}
	return nil
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
		batch := m.claimBatch(ctx)
		if len(batch) == 0 {
			break
		}
		m.dispatchBatch(ctx, batch)
	}
	m.refreshGauges(ctx)
}

func (m *StorageMiddleware) claimBatch(ctx context.Context) []*OutboxEntry {
	var batch []*OutboxEntry
	for len(batch) < m.dispatcher.BatchSize {
		entry, claimed, err := m.claim(ctx)
		if err != nil {
			slog.WarnContext(ctx, "Failed to claim notification entry", "outboxID", m.outboxID, "error", err)
			break
		}
		if entry == nil || !claimed {
			break
		}
		m.metrics.recordClaimed(m.outboxID, entry)
		batch = append(batch, entry)
	}
	return batch
}

func (m *StorageMiddleware) refreshGauges(ctx context.Context) {
	err := database.WithTx(ctx, m.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		pending, err := m.repository.CountPending(ctx, tx.SqlTx(), m.outboxID)
		if err != nil {
			return err
		}
		deadLettered, err := m.repository.CountDeadLettered(ctx, tx.SqlTx(), m.outboxID)
		if err != nil {
			return err
		}
		m.metrics.setPending(m.outboxID, pending, deadLettered)
		return nil
	})
	if err != nil {
		slog.DebugContext(ctx, "Failed to refresh notification outbox gauges", "outboxID", m.outboxID, "error", err)
	}
}

func (m *StorageMiddleware) dispatchBatch(ctx context.Context, batch []*OutboxEntry) {
	if len(batch) == 1 || m.dispatcher.Concurrency == 1 {
		for _, entry := range batch {
			m.dispatchEntry(ctx, entry)
		}
		return
	}
	semaphore := make(chan struct{}, m.dispatcher.Concurrency)
	var wg sync.WaitGroup
	for _, entry := range batch {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(entry *OutboxEntry) {
			defer wg.Done()
			defer func() { <-semaphore }()
			m.dispatchEntry(ctx, entry)
		}(entry)
	}
	wg.Wait()
}

func (m *StorageMiddleware) dispatchEntry(ctx context.Context, entry *OutboxEntry) {
	start := time.Now()
	err := m.publisher.Publish(ctx, entry)
	latency := time.Since(start)
	if err == nil {
		m.metrics.recordPublished(m.outboxID, entry, latency)
		slog.DebugContext(ctx, "Published notification", "outboxID", m.outboxID, "entryID", entry.ID.String(), "eventName", entry.EventName, "destinationType", destinationTypeLabel(entry.DestinationARN), "attempts", entry.Attempts)
		_ = m.deleteClaimed(ctx, entry)
		return
	}
	m.metrics.recordFailed(m.outboxID, entry, latency)
	if m.dispatcher.MaxAttempts > 0 && entry.Attempts >= m.dispatcher.MaxAttempts {
		slog.ErrorContext(ctx, "Dead-lettering notification after exhausting attempts", "outboxID", m.outboxID, "entryID", entry.ID.String(), "eventName", entry.EventName, "destinationType", destinationTypeLabel(entry.DestinationARN), "attempts", entry.Attempts, "error", err)
		_ = m.deadLetter(ctx, entry, err)
		return
	}
	nextRetry := m.nextAttemptAt(entry)
	m.metrics.recordRetry(m.outboxID, entry)
	slog.WarnContext(ctx, "Failed to publish notification, will retry", "outboxID", m.outboxID, "entryID", entry.ID.String(), "eventName", entry.EventName, "destinationType", destinationTypeLabel(entry.DestinationARN), "attempts", entry.Attempts, "nextRetryAt", nextRetry, "error", err)
	_ = m.release(ctx, entry, nextRetry, err)
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

func (m *StorageMiddleware) nextAttemptAt(entry *OutboxEntry) time.Time {
	// entry.Attempts has already been incremented by the claim, so the first
	// retry waits minBackoff.
	exponent := entry.Attempts - 1
	if exponent < 0 {
		exponent = 0
	}
	delay := float64(m.dispatcher.MinBackoff) * math.Pow(2, float64(exponent))
	maxBackoff := float64(m.dispatcher.MaxBackoff)
	if delay > maxBackoff || math.IsInf(delay, 1) {
		delay = maxBackoff
	}
	return time.Now().UTC().Add(time.Duration(delay))
}

func (m *StorageMiddleware) release(ctx context.Context, entry *OutboxEntry, nextAttemptAt time.Time, cause error) error {
	return database.WithTx(ctx, m.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		_, err := m.repository.ReleaseClaim(ctx, tx.SqlTx(), m.outboxID, *entry.ID, m.claimOwner, nextAttemptAt, time.Now().UTC(), errorMessage(cause))
		return err
	})
}

func (m *StorageMiddleware) deadLetter(ctx context.Context, entry *OutboxEntry, cause error) error {
	return database.WithTx(ctx, m.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		_, err := m.repository.DeadLetter(ctx, tx.SqlTx(), m.outboxID, *entry.ID, m.claimOwner, time.Now().UTC(), errorMessage(cause))
		return err
	})
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// destinationTypeLabel derives a low-cardinality destination type label from an
// entry ARN for logs and metrics, avoiding per-destination cardinality.
func destinationTypeLabel(destinationARN string) string {
	if strings.HasPrefix(destinationARN, eventBridgeARNPrefix) {
		return "eventbridge"
	}
	if arn, err := parseARN(destinationARN); err == nil {
		return arn.Service
	}
	return "unknown"
}
