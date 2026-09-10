package pgx

import (
	"context"
	"database/sql"
	"errors"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/objectlock"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/oklog/ulid/v2"
)

type repository struct{}

func NewRepository() (objectlock.Repository, error) { return &repository{}, nil }

// LockBucket precedes object and part locks and holds the row until commit.
func (r *repository) LockBucket(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName) error {
	var found string
	err := tx.QueryRowContext(ctx, "SELECT name FROM buckets WHERE name = $1 FOR UPDATE", name.String()).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return metadatastore.ErrNoSuchBucket
	}
	return err
}
func (r *repository) LockObject(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	var found string
	return tx.QueryRowContext(ctx, "SELECT id FROM objects WHERE id = $1 FOR UPDATE", id.String()).Scan(&found)
}

func (r *repository) FindBucketConfiguration(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName) (*metadatastore.ObjectLockConfiguration, error) {
	var mode sql.NullString
	var days, years sql.NullInt32
	err := tx.QueryRowContext(ctx, `SELECT default_retention_mode, default_retention_days, default_retention_years FROM bucket_object_lock_configurations WHERE bucket_name = $1`, name.String()).Scan(&mode, &days, &years)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	config := &metadatastore.ObjectLockConfiguration{ObjectLockEnabled: "Enabled"}
	if mode.Valid || days.Valid || years.Valid {
		config.DefaultRetention = &metadatastore.DefaultRetention{Mode: metadatastore.RetentionMode(mode.String)}
		if days.Valid {
			config.DefaultRetention.Days = &days.Int32
		}
		if years.Valid {
			config.DefaultRetention.Years = &years.Int32
		}
	}
	return config, config.Validate()
}

func (r *repository) SaveBucketConfiguration(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName, config *metadatastore.ObjectLockConfiguration) error {
	if err := config.Validate(); err != nil {
		return err
	}
	var mode, days, years any
	if d := config.DefaultRetention; d != nil {
		mode, days, years = string(d.Mode), d.Days, d.Years
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO bucket_object_lock_configurations (bucket_name, default_retention_mode, default_retention_days, default_retention_years) VALUES ($1, $2, $3, $4) ON CONFLICT (bucket_name) DO UPDATE SET default_retention_mode = excluded.default_retention_mode, default_retention_days = excluded.default_retention_days, default_retention_years = excluded.default_retention_years`, name.String(), mode, days, years)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE buckets SET versioning_status = 'Enabled' WHERE name = $1`, name.String())
	return err
}

func (r *repository) FindObjectLock(ctx context.Context, tx *sql.Tx, id ulid.ULID) (metadatastore.ObjectLock, error) {
	var mode, hold sql.NullString
	var until sql.NullTime
	lock := metadatastore.ObjectLock{}
	err := tx.QueryRowContext(ctx, `SELECT retention_mode, retain_until_date, legal_hold_status FROM object_locks WHERE object_id = $1`, id.String()).Scan(&mode, &until, &hold)
	if errors.Is(err, sql.ErrNoRows) {
		return lock, nil
	}
	if err != nil {
		return lock, err
	}
	if mode.Valid || until.Valid {
		lock.Retention = &metadatastore.ObjectRetention{Mode: metadatastore.RetentionMode(mode.String), RetainUntilDate: until.Time.UTC()}
	}
	if hold.Valid {
		status := metadatastore.LegalHoldStatus(hold.String)
		lock.LegalHold = &status
	}
	return lock, lock.Validate()
}

func (r *repository) SaveObjectLock(ctx context.Context, tx *sql.Tx, id ulid.ULID, lock metadatastore.ObjectLock) error {
	if err := lock.Validate(); err != nil {
		return err
	}
	if lock.Retention == nil && lock.LegalHold == nil {
		_, err := tx.ExecContext(ctx, `DELETE FROM object_locks WHERE object_id = $1`, id.String())
		return err
	}
	var mode, until any
	if r := lock.Retention; r != nil {
		mode, until = string(r.Mode), r.RetainUntilDate.UTC()
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO object_locks (object_id, retention_mode, retain_until_date, legal_hold_status) VALUES ($1, $2, $3, $4) ON CONFLICT (object_id) DO UPDATE SET retention_mode = excluded.retention_mode, retain_until_date = excluded.retain_until_date, legal_hold_status = excluded.legal_hold_status`, id.String(), mode, until, lock.LegalHold)
	return err
}
