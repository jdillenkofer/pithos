package notification

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type OutboxEntry struct {
	ID             *ulid.ULID
	DestinationARN string
	EventName      string
	PayloadFormat  PayloadFormat
	Payload        []byte
	Attempts       int
	NextAttemptAt  time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ClaimOwner     *string
	ClaimUntil     *time.Time
	Version        int64
}

type Repository interface {
	Count(ctx context.Context, tx *sql.Tx, outboxID string) (int, error)
	CountPending(ctx context.Context, tx *sql.Tx, outboxID string) (int, error)
	CountDeadLettered(ctx context.Context, tx *sql.Tx, outboxID string) (int, error)
	Save(ctx context.Context, tx *sql.Tx, outboxID string, entry *OutboxEntry) error
	ClaimFirst(ctx context.Context, tx *sql.Tx, outboxID string, owner string, now time.Time, claimUntil time.Time) (*OutboxEntry, bool, error)
	DeleteByClaimOwner(ctx context.Context, tx *sql.Tx, outboxID string, id ulid.ULID, owner string) (bool, error)
	ReleaseClaim(ctx context.Context, tx *sql.Tx, outboxID string, id ulid.ULID, owner string, nextAttemptAt time.Time, now time.Time, lastError string) (bool, error)
	DeadLetter(ctx context.Context, tx *sql.Tx, outboxID string, id ulid.ULID, owner string, now time.Time, lastError string) (bool, error)
	ExtendClaim(ctx context.Context, tx *sql.Tx, outboxID string, id ulid.ULID, owner string, now time.Time, claimUntil time.Time) (bool, error)
}

type SQLRepository struct{}

const (
	countStmt             = "SELECT COUNT(*) FROM notification_outbox_entries WHERE outbox_id = $1"
	countPendingStmt      = "SELECT COUNT(*) FROM notification_outbox_entries WHERE outbox_id = $1 AND dead_lettered_at IS NULL"
	countDeadLetteredStmt = "SELECT COUNT(*) FROM notification_outbox_entries WHERE outbox_id = $1 AND dead_lettered_at IS NOT NULL"
	insertStmt            = "INSERT INTO notification_outbox_entries (id, outbox_id, destination_arn, event_name, payload_format, payload, attempts, next_attempt_at, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	findFirstStmt         = "SELECT id, destination_arn, event_name, payload_format, payload, attempts, next_attempt_at, created_at, updated_at, claim_owner, claim_until, version FROM notification_outbox_entries WHERE outbox_id = $1 AND dead_lettered_at IS NULL AND next_attempt_at <= $2 AND (claim_owner IS NULL OR claim_until <= $2) ORDER BY next_attempt_at ASC, id ASC LIMIT 1"
	claimStmt             = "UPDATE notification_outbox_entries SET claim_owner = $1, claim_until = $2, attempts = attempts + 1, version = version + 1, updated_at = $3 WHERE id = $4 AND outbox_id = $5 AND version = $6 AND dead_lettered_at IS NULL AND next_attempt_at <= $7 AND (claim_owner IS NULL OR claim_until <= $7)"
	deleteClaimStmt       = "DELETE FROM notification_outbox_entries WHERE id = $1 AND outbox_id = $2 AND claim_owner = $3"
	releaseClaimStmt      = "UPDATE notification_outbox_entries SET claim_owner = NULL, claim_until = NULL, next_attempt_at = $1, version = version + 1, updated_at = $2, last_error = $3, last_error_at = $4 WHERE id = $5 AND outbox_id = $6 AND claim_owner = $7"
	deadLetterStmt        = "UPDATE notification_outbox_entries SET claim_owner = NULL, claim_until = NULL, dead_lettered_at = $1, version = version + 1, updated_at = $2, last_error = $3, last_error_at = $4 WHERE id = $5 AND outbox_id = $6 AND claim_owner = $7"
	extendClaimStmt       = "UPDATE notification_outbox_entries SET claim_until = $1, version = version + 1, updated_at = $2 WHERE id = $3 AND outbox_id = $4 AND claim_owner = $5"
)

func NewSQLRepository() *SQLRepository {
	return &SQLRepository{}
}

func (r *SQLRepository) Count(ctx context.Context, tx *sql.Tx, outboxID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, countStmt, outboxID).Scan(&count)
	return count, err
}

func (r *SQLRepository) CountPending(ctx context.Context, tx *sql.Tx, outboxID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, countPendingStmt, outboxID).Scan(&count)
	return count, err
}

func (r *SQLRepository) CountDeadLettered(ctx context.Context, tx *sql.Tx, outboxID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, countDeadLetteredStmt, outboxID).Scan(&count)
	return count, err
}

func (r *SQLRepository) Save(ctx context.Context, tx *sql.Tx, outboxID string, entry *OutboxEntry) error {
	if entry.ID == nil {
		id := ulid.Make()
		now := time.Now().UTC()
		entry.ID = &id
		entry.CreatedAt = now
		entry.UpdatedAt = now
		if entry.NextAttemptAt.IsZero() {
			entry.NextAttemptAt = now
		}
	}
	_, err := tx.ExecContext(ctx, insertStmt, entry.ID.String(), outboxID, entry.DestinationARN, entry.EventName, string(entry.PayloadFormat), entry.Payload, entry.Attempts, entry.NextAttemptAt, entry.CreatedAt, entry.UpdatedAt)
	return err
}

func (r *SQLRepository) ClaimFirst(ctx context.Context, tx *sql.Tx, outboxID string, owner string, now time.Time, claimUntil time.Time) (*OutboxEntry, bool, error) {
	entry, err := scanEntry(tx.QueryRowContext(ctx, findFirstStmt, outboxID, now))
	if err != nil || entry == nil {
		return entry, false, err
	}
	res, err := tx.ExecContext(ctx, claimStmt, owner, claimUntil, now, entry.ID.String(), outboxID, entry.Version, now)
	if err != nil {
		return nil, false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if affected != 1 {
		return nil, false, nil
	}
	entry.ClaimOwner = &owner
	entry.ClaimUntil = &claimUntil
	entry.Attempts++
	entry.Version++
	entry.UpdatedAt = now
	return entry, true, nil
}

func (r *SQLRepository) DeleteByClaimOwner(ctx context.Context, tx *sql.Tx, outboxID string, id ulid.ULID, owner string) (bool, error) {
	res, err := tx.ExecContext(ctx, deleteClaimStmt, id.String(), outboxID, owner)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	return affected == 1, err
}

func (r *SQLRepository) ReleaseClaim(ctx context.Context, tx *sql.Tx, outboxID string, id ulid.ULID, owner string, nextAttemptAt time.Time, now time.Time, lastError string) (bool, error) {
	res, err := tx.ExecContext(ctx, releaseClaimStmt, nextAttemptAt, now, nullableString(lastError), now, id.String(), outboxID, owner)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	return affected == 1, err
}

func (r *SQLRepository) DeadLetter(ctx context.Context, tx *sql.Tx, outboxID string, id ulid.ULID, owner string, now time.Time, lastError string) (bool, error) {
	res, err := tx.ExecContext(ctx, deadLetterStmt, now, now, nullableString(lastError), now, id.String(), outboxID, owner)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	return affected == 1, err
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (r *SQLRepository) ExtendClaim(ctx context.Context, tx *sql.Tx, outboxID string, id ulid.ULID, owner string, now time.Time, claimUntil time.Time) (bool, error) {
	res, err := tx.ExecContext(ctx, extendClaimStmt, claimUntil, now, id.String(), outboxID, owner)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	return affected == 1, err
}

func scanEntry(row *sql.Row) (*OutboxEntry, error) {
	var id string
	var payloadFormat string
	entry := &OutboxEntry{}
	err := row.Scan(&id, &entry.DestinationARN, &entry.EventName, &payloadFormat, &entry.Payload, &entry.Attempts, &entry.NextAttemptAt, &entry.CreatedAt, &entry.UpdatedAt, &entry.ClaimOwner, &entry.ClaimUntil, &entry.Version)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ulidID := ulid.MustParse(id)
	entry.ID = &ulidID
	entry.PayloadFormat = PayloadFormat(payloadFormat)
	return entry, nil
}
