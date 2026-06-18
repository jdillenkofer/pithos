package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/webhookoutboxentry"
	"github.com/oklog/ulid/v2"
)

type sqliteRepository struct {
}

const (
	countWebhookOutboxEntriesStmt          = "SELECT COUNT(*) FROM webhook_outbox_entries WHERE outbox_id = $1"
	findFirstWebhookOutboxEntryStmt        = "SELECT id, url, method, headers, body, created_at, updated_at, claim_owner, claim_until, version FROM webhook_outbox_entries WHERE outbox_id = $1 ORDER BY id ASC LIMIT 1"
	insertWebhookOutboxEntryStmt           = "INSERT INTO webhook_outbox_entries (id, outbox_id, url, method, headers, body, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8)"
	updateWebhookOutboxEntryByIdStmt       = "UPDATE webhook_outbox_entries SET url = $1, method = $2, headers = $3, body = $4, updated_at = $5 WHERE id = $6 AND outbox_id = $7"
	claimWebhookOutboxEntryStmt            = "UPDATE webhook_outbox_entries SET claim_owner = $1, claim_until = $2, version = version + 1, updated_at = $3 WHERE id = $4 AND outbox_id = $5 AND version = $6 AND (claim_owner IS NULL OR claim_until <= $7)"
	deleteWebhookOutboxEntryByClaimOwner   = "DELETE FROM webhook_outbox_entries WHERE id = $1 AND outbox_id = $2 AND claim_owner = $3"
	releaseWebhookOutboxEntryClaimStmt     = "UPDATE webhook_outbox_entries SET claim_owner = NULL, claim_until = NULL, version = version + 1, updated_at = $1 WHERE id = $2 AND outbox_id = $3 AND claim_owner = $4"
	extendWebhookOutboxEntryClaimStmt      = "UPDATE webhook_outbox_entries SET claim_until = $1, version = version + 1, updated_at = $2 WHERE id = $3 AND outbox_id = $4 AND claim_owner = $5"
)

func NewRepository() (webhookoutboxentry.Repository, error) {
	return &sqliteRepository{}, nil
}

func (wor *sqliteRepository) Count(ctx context.Context, tx *sql.Tx, outboxId string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, countWebhookOutboxEntriesStmt, outboxId).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func convertRowToWebhookOutboxEntryEntity(webhookOutboxRow *sql.Row) (*webhookoutboxentry.Entity, error) {
	var id string
	var url string
	var method string
	var headers *string
	var body []byte
	var createdAt time.Time
	var updatedAt time.Time
	var claimOwner *string
	var claimUntil *time.Time
	var version int64
	err := webhookOutboxRow.Scan(&id, &url, &method, &headers, &body, &createdAt, &updatedAt, &claimOwner, &claimUntil, &version)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	return &webhookoutboxentry.Entity{
		Id:         &ulidId,
		Url:        url,
		Method:     method,
		Headers:    headers,
		Body:       body,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
		ClaimOwner: claimOwner,
		ClaimUntil: claimUntil,
		Version:    version,
	}, nil
}

func (wor *sqliteRepository) FindFirstWebhookOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*webhookoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstWebhookOutboxEntryStmt, outboxId)
	return convertRowToWebhookOutboxEntryEntity(row)
}

func (wor *sqliteRepository) SaveWebhookOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, webhookOutboxEntry *webhookoutboxentry.Entity) error {
	if webhookOutboxEntry.Id == nil {
		id := ulid.Make()
		webhookOutboxEntry.Id = &id
		webhookOutboxEntry.CreatedAt = time.Now().UTC()
		webhookOutboxEntry.UpdatedAt = webhookOutboxEntry.CreatedAt
		_, err := tx.ExecContext(ctx, insertWebhookOutboxEntryStmt, webhookOutboxEntry.Id.String(), outboxId, webhookOutboxEntry.Url, webhookOutboxEntry.Method, webhookOutboxEntry.Headers, webhookOutboxEntry.Body, webhookOutboxEntry.CreatedAt, webhookOutboxEntry.UpdatedAt)
		return err
	}

	webhookOutboxEntry.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateWebhookOutboxEntryByIdStmt, webhookOutboxEntry.Url, webhookOutboxEntry.Method, webhookOutboxEntry.Headers, webhookOutboxEntry.Body, webhookOutboxEntry.UpdatedAt, webhookOutboxEntry.Id.String(), outboxId)
	return err
}

func (wor *sqliteRepository) ClaimFirstWebhookOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, owner string, now time.Time, claimUntil time.Time) (*webhookoutboxentry.Entity, bool, error) {
	entry, err := wor.FindFirstWebhookOutboxEntry(ctx, tx, outboxId)
	if err != nil || entry == nil {
		return entry, false, err
	}
	result, err := tx.ExecContext(ctx, claimWebhookOutboxEntryStmt, owner, claimUntil, now, entry.Id.String(), outboxId, entry.Version, now)
	if err != nil {
		return nil, false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if affected != 1 {
		return nil, false, nil
	}
	entry.ClaimOwner = &owner
	entry.ClaimUntil = &claimUntil
	entry.Version += 1
	entry.UpdatedAt = now
	return entry, true, nil
}

func (wor *sqliteRepository) DeleteWebhookOutboxEntryByClaimOwner(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string) (bool, error) {
	result, err := tx.ExecContext(ctx, deleteWebhookOutboxEntryByClaimOwner, id.String(), outboxId, owner)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (wor *sqliteRepository) ReleaseWebhookOutboxEntryClaim(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string, now time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, releaseWebhookOutboxEntryClaimStmt, now, id.String(), outboxId, owner)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (wor *sqliteRepository) ExtendWebhookOutboxEntryClaim(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string, now time.Time, claimUntil time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, extendWebhookOutboxEntryClaimStmt, claimUntil, now, id.String(), outboxId, owner)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}
