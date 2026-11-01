package partreaderlease

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partreaderlease"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type repository struct{}

const (
	insertLeaseStmt     = "INSERT INTO part_reader_lease (id, expires_at) VALUES ($1, clock_timestamp() + $2 * INTERVAL '1 millisecond') ON CONFLICT (id) DO UPDATE SET expires_at = EXCLUDED.expires_at"
	insertLeasePartStmt = "INSERT INTO part_reader_lease_part (lease_id, part_id) SELECT $1, UNNEST($2::TEXT[]) ON CONFLICT DO NOTHING"
	renewLeasesStmt     = "UPDATE part_reader_lease SET expires_at = clock_timestamp() + $2 * INTERVAL '1 millisecond' WHERE id = ANY($1::TEXT[]) AND expires_at > clock_timestamp()"
	deleteLeasesStmt    = "DELETE FROM part_reader_lease WHERE id = ANY($1::TEXT[])"
	// findLeasedStmt compares expiry with a time no later than its snapshot.
	// Measured at evaluation time instead, a renewal committed after the
	// snapshot but before the expiry the snapshot still shows would be missed,
	// although it kept the reader protected. An earlier time only keeps more
	// leases, which is safe.
	findLeasedStmt    = "SELECT DISTINCT lp.part_id FROM part_reader_lease_part lp JOIN part_reader_lease l ON l.id = lp.lease_id WHERE lp.part_id = ANY($1::TEXT[]) AND l.expires_at > transaction_timestamp()"
	deleteExpiredStmt = "DELETE FROM part_reader_lease WHERE expires_at <= clock_timestamp()"
)

func NewRepository() (partreaderlease.Repository, error) {
	return &repository{}, nil
}

func partIdStrings(partIds []partstore.PartId) []string {
	ids := make([]string, len(partIds))
	for i, id := range partIds {
		ids[i] = id.String()
	}
	return ids
}

func (r *repository) Acquire(ctx context.Context, tx *sql.Tx, leaseId string, partIds []partstore.PartId, ttl time.Duration) error {
	if _, err := tx.ExecContext(ctx, insertLeaseStmt, leaseId, ttl.Milliseconds()); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, insertLeasePartStmt, leaseId, partIdStrings(partIds))
	return err
}

func (r *repository) Renew(ctx context.Context, tx *sql.Tx, leaseIds []string, ttl time.Duration) error {
	_, err := tx.ExecContext(ctx, renewLeasesStmt, leaseIds, ttl.Milliseconds())
	return err
}

func (r *repository) Release(ctx context.Context, tx *sql.Tx, leaseIds []string) error {
	_, err := tx.ExecContext(ctx, deleteLeasesStmt, leaseIds)
	return err
}

func (r *repository) FindLeased(ctx context.Context, tx *sql.Tx, partIds []partstore.PartId) ([]partstore.PartId, error) {
	rows, err := tx.QueryContext(ctx, findLeasedStmt, partIdStrings(partIds))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var leased []partstore.PartId
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		leased = append(leased, *partstore.MustNewPartIdFromString(id))
	}
	return leased, rows.Err()
}

func (r *repository) DeleteExpired(ctx context.Context, tx *sql.Tx) (int64, error) {
	result, err := tx.ExecContext(ctx, deleteExpiredStmt)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
