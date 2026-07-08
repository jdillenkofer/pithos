package notification

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

type controllablePublisher struct {
	mu            sync.Mutex
	calls         int
	failAll       bool
	delay         time.Duration
	concurrent    int32
	maxConcurrent int32
}

func (p *controllablePublisher) Publish(ctx context.Context, entry *OutboxEntry) error {
	cur := atomic.AddInt32(&p.concurrent, 1)
	defer atomic.AddInt32(&p.concurrent, -1)
	for {
		mx := atomic.LoadInt32(&p.maxConcurrent)
		if cur <= mx || atomic.CompareAndSwapInt32(&p.maxConcurrent, mx, cur) {
			break
		}
	}
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.failAll {
		return errors.New("simulated publish failure")
	}
	return nil
}

func (p *controllablePublisher) Validate(ctx context.Context, arn string, destination Destination) error {
	return nil
}

func dispatcherStack(t *testing.T, publisher Publisher, dispatcher DispatcherConfig) *StorageMiddleware {
	t.Helper()
	db := openTestDB(t)
	inner := newSharedDBMetadataStorage(t, db)
	mw, err := NewStorageMiddleware(inner, db, NewSQLRepository(), publisher, "default", time.Minute, dispatcher, nil)
	require.NoError(t, err)
	return mw
}

func seedEntry(t *testing.T, mw *StorageMiddleware, arn string) ulid.ULID {
	t.Helper()
	var id ulid.ULID
	require.NoError(t, database.WithTx(context.Background(), mw.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		entry := &OutboxEntry{DestinationARN: arn, EventName: EventObjectCreatedPut, PayloadFormat: PayloadFormatS3Records, Payload: []byte("{}")}
		if err := mw.repository.Save(ctx, tx.SqlTx(), mw.outboxID, entry); err != nil {
			return err
		}
		id = *entry.ID
		return nil
	}))
	return id
}

type outboxRow struct {
	Attempts       int
	LastError      sql.NullString
	NextAttemptAt  time.Time
	DeadLetteredAt sql.NullTime
}

func readOutboxRow(t *testing.T, mw *StorageMiddleware, id ulid.ULID) outboxRow {
	t.Helper()
	var row outboxRow
	require.NoError(t, database.WithTx(context.Background(), mw.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		return tx.SqlTx().QueryRowContext(ctx,
			"SELECT attempts, last_error, next_attempt_at, dead_lettered_at FROM notification_outbox_entries WHERE id = $1",
			id.String(),
		).Scan(&row.Attempts, &row.LastError, &row.NextAttemptAt, &row.DeadLetteredAt)
	}))
	return row
}

func TestDispatcherRetriesWithBackoffAndPersistsLastError(t *testing.T) {
	ctx := context.Background()
	publisher := &controllablePublisher{failAll: true}
	mw := dispatcherStack(t, publisher, DispatcherConfig{MaxAttempts: 5, MinBackoff: 2 * time.Second})
	id := seedEntry(t, mw, "arn:aws:sqs:eu-central-1:000000000000:queue")

	before := time.Now().UTC()
	mw.dispatchAvailable(ctx)

	require.Equal(t, 1, publisher.calls, "a released entry must not be retried until its backoff elapses")
	row := readOutboxRow(t, mw, id)
	require.Equal(t, 1, row.Attempts)
	require.True(t, row.LastError.Valid)
	require.Contains(t, row.LastError.String, "simulated publish failure")
	require.False(t, row.DeadLetteredAt.Valid)
	require.True(t, row.NextAttemptAt.After(before.Add(time.Second)), "next attempt should be delayed by the backoff")
}

func TestDispatcherDeadLettersAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	publisher := &controllablePublisher{failAll: true}
	mw := dispatcherStack(t, publisher, DispatcherConfig{MaxAttempts: 1, MinBackoff: time.Millisecond})
	id := seedEntry(t, mw, "arn:aws:sqs:eu-central-1:000000000000:queue")

	mw.dispatchAvailable(ctx)

	row := readOutboxRow(t, mw, id)
	require.Equal(t, 1, row.Attempts)
	require.True(t, row.DeadLetteredAt.Valid, "entry must be dead-lettered after exhausting attempts")
	require.True(t, row.LastError.Valid)

	require.Equal(t, 0, countPending(t, mw))
	require.Equal(t, 1, countDeadLettered(t, mw))
}

func TestDispatcherIgnoresDeadLetteredRows(t *testing.T) {
	ctx := context.Background()
	publisher := &controllablePublisher{failAll: true}
	mw := dispatcherStack(t, publisher, DispatcherConfig{MaxAttempts: 1, MinBackoff: time.Millisecond})
	seedEntry(t, mw, "arn:aws:sqs:eu-central-1:000000000000:queue")

	mw.dispatchAvailable(ctx)
	callsAfterDeadLetter := publisher.calls

	// A dead-lettered row must never be claimed again.
	entry, claimed, err := mw.claim(ctx)
	require.NoError(t, err)
	require.Nil(t, entry)
	require.False(t, claimed)

	mw.dispatchAvailable(ctx)
	require.Equal(t, callsAfterDeadLetter, publisher.calls, "dead-lettered rows must not be retried")
}

func TestDispatcherClaimsBatch(t *testing.T) {
	ctx := context.Background()
	publisher := &controllablePublisher{}
	mw := dispatcherStack(t, publisher, DispatcherConfig{BatchSize: 3})
	for i := 0; i < 5; i++ {
		seedEntry(t, mw, "arn:aws:sqs:eu-central-1:000000000000:queue")
	}

	batch := mw.claimBatch(ctx)
	require.Len(t, batch, 3, "claimBatch must not exceed the configured batch size")
}

func TestDispatcherPublishesConcurrently(t *testing.T) {
	ctx := context.Background()
	publisher := &controllablePublisher{delay: 25 * time.Millisecond}
	mw := dispatcherStack(t, publisher, DispatcherConfig{Concurrency: 4, BatchSize: 8})
	for i := 0; i < 8; i++ {
		seedEntry(t, mw, "arn:aws:sqs:eu-central-1:000000000000:queue")
	}

	mw.dispatchAvailable(ctx)

	require.Equal(t, 8, publisher.calls)
	require.GreaterOrEqual(t, atomic.LoadInt32(&publisher.maxConcurrent), int32(2), "entries should be published concurrently")
	require.Equal(t, 0, countPending(t, mw), "successfully published entries are deleted")
}

func TestDispatcherIsolatesByOutboxID(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	inner := newSharedDBMetadataStorage(t, db)
	publisherA := &controllablePublisher{}
	publisherB := &controllablePublisher{}
	mwA, err := NewStorageMiddleware(inner, db, NewSQLRepository(), publisherA, "outbox-a", time.Minute, DispatcherConfig{BatchSize: 10}, nil)
	require.NoError(t, err)
	mwB, err := NewStorageMiddleware(inner, db, NewSQLRepository(), publisherB, "outbox-b", time.Minute, DispatcherConfig{BatchSize: 10}, nil)
	require.NoError(t, err)

	seedEntry(t, mwA, "arn:aws:sqs:eu-central-1:000000000000:a")
	seedEntry(t, mwA, "arn:aws:sqs:eu-central-1:000000000000:a")
	seedEntry(t, mwB, "arn:aws:sqs:eu-central-1:000000000000:b")

	mwA.dispatchAvailable(ctx)
	require.Equal(t, 2, publisherA.calls, "outbox-a dispatcher must claim only its own rows")
	require.Equal(t, 0, publisherB.calls)
	require.Equal(t, 1, countPending(t, mwB), "outbox-b rows must remain untouched")

	mwB.dispatchAvailable(ctx)
	require.Equal(t, 1, publisherB.calls)
	require.Equal(t, 0, countPending(t, mwA))
	require.Equal(t, 0, countPending(t, mwB))
}

func countPending(t *testing.T, mw *StorageMiddleware) int {
	t.Helper()
	var count int
	require.NoError(t, database.WithTx(context.Background(), mw.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		count, err = mw.repository.CountPending(ctx, tx.SqlTx(), mw.outboxID)
		return err
	}))
	return count
}

func countDeadLettered(t *testing.T, mw *StorageMiddleware) int {
	t.Helper()
	var count int
	require.NoError(t, database.WithTx(context.Background(), mw.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		count, err = mw.repository.CountDeadLettered(ctx, tx.SqlTx(), mw.outboxID)
		return err
	}))
	return count
}
