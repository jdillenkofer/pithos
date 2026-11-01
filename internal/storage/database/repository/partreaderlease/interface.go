package partreaderlease

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// Repository persists reader leases, which keep GC from condemning the
// parts that long-running readers still stream. Expiry is measured on the
// database clock, so instances need not agree on wall-clock time.
type Repository interface {
	// Acquire records a lease on partIds that expires ttl from now. It is
	// idempotent, so a retry after a commit whose outcome was lost succeeds.
	Acquire(ctx context.Context, tx *sql.Tx, leaseId string, partIds []partstore.PartId, ttl time.Duration) error
	// Renew extends the unexpired leases among leaseIds to ttl from now.
	// Expired leases stay expired: GC may have ignored them already.
	Renew(ctx context.Context, tx *sql.Tx, leaseIds []string, ttl time.Duration) error
	Release(ctx context.Context, tx *sql.Tx, leaseIds []string) error
	// FindLeased returns the subset of partIds held by an unexpired lease.
	FindLeased(ctx context.Context, tx *sql.Tx, partIds []partstore.PartId) ([]partstore.PartId, error)
	DeleteExpired(ctx context.Context, tx *sql.Tx) (int64, error)
}
