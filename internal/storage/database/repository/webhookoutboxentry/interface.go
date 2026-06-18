package webhookoutboxentry

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type Repository interface {
	Count(ctx context.Context, tx *sql.Tx, outboxId string) (int, error)
	FindFirstWebhookOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*Entity, error)
	SaveWebhookOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, webhookOutboxEntry *Entity) error
	ClaimFirstWebhookOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, owner string, now time.Time, claimUntil time.Time) (*Entity, bool, error)
	DeleteWebhookOutboxEntryByClaimOwner(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string) (bool, error)
	ReleaseWebhookOutboxEntryClaim(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string, now time.Time) (bool, error)
	ExtendWebhookOutboxEntryClaim(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string, now time.Time, claimUntil time.Time) (bool, error)
}

type Entity struct {
	Id         *ulid.ULID
	Url        string
	Method     string
	Headers    *string
	Body       []byte
	CreatedAt  time.Time
	UpdatedAt  time.Time
	ClaimOwner *string
	ClaimUntil *time.Time
	Version    int64
}
