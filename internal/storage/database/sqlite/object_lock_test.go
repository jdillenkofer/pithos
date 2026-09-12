package sqlite

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectLockSchema(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	m, err := createMigrateInstance(db)
	require.NoError(t, err)
	require.NoError(t, m.Migrate(38))
	_, err = db.Exec(`INSERT INTO buckets (id, name, created_at, updated_at) VALUES ('bucket-id', 'bucket', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO objects (id, bucket_name, key, etag, size, upload_status, created_at, updated_at)
		VALUES ('old-version', 'bucket', 'key', 'etag', 0, 'COMPLETED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	require.NoError(t, err)
	require.NoError(t, m.Migrate(39))
	_, err = db.Exec(`PRAGMA foreign_keys = ON`)
	require.NoError(t, err)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM object_locks`).Scan(&count))
	require.Zero(t, count, "migration must not retroactively protect versions")
	var enabled bool
	require.NoError(t, db.QueryRow(`SELECT object_lock_enabled FROM buckets WHERE name = 'bucket'`).Scan(&enabled))
	require.False(t, enabled, "migration must not enable Object Lock")

	for _, tc := range []struct {
		name              string
		mode, days, years any
		valid             bool
	}{
		{"no default", nil, nil, nil, true},
		{"days", "GOVERNANCE", 1, nil, true},
		{"years", "COMPLIANCE", nil, 1, true},
		{"missing duration", "GOVERNANCE", nil, nil, false},
		{"missing mode", nil, 1, nil, false},
		{"both durations", "GOVERNANCE", 1, 1, false},
		{"zero days", "GOVERNANCE", 0, nil, false},
		{"negative years", "COMPLIANCE", nil, -1, false},
		{"invalid mode", "invalid", 1, nil, false},
	} {
		t.Run("bucket/"+tc.name, func(t *testing.T) {
			_, err := db.Exec(`UPDATE buckets SET object_lock_enabled = TRUE, default_retention_mode = ?, default_retention_days = ?, default_retention_years = ? WHERE name = 'bucket'`, tc.mode, tc.days, tc.years)
			if tc.valid {
				require.NoError(t, err)
				_, err = db.Exec(`UPDATE buckets SET object_lock_enabled = FALSE, default_retention_mode = NULL, default_retention_days = NULL, default_retention_years = NULL WHERE name = 'bucket'`)
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	for _, tc := range []struct {
		name              string
		mode, until, hold any
		valid             bool
	}{
		{"empty", nil, nil, nil, true},
		{"retention", "COMPLIANCE", "2027-01-01 00:00:00", nil, true},
		{"hold only", nil, nil, "ON", true},
		{"both", "GOVERNANCE", "2027-01-01 00:00:00", "ON", true},
		{"hold off", nil, nil, "OFF", true},
		{"missing date", "COMPLIANCE", nil, nil, false},
		{"missing mode", nil, "2027-01-01 00:00:00", nil, false},
		{"invalid mode", "invalid", "2027-01-01 00:00:00", nil, false},
		{"invalid hold", nil, nil, "invalid", false},
	} {
		t.Run("version/"+tc.name, func(t *testing.T) {
			_, err := db.Exec(`INSERT INTO object_locks VALUES ('old-version', ?, ?, ?)`, tc.mode, tc.until, tc.hold)
			if tc.valid {
				require.NoError(t, err)
				_, err = db.Exec(`DELETE FROM object_locks`)
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	_, err = db.Exec(`INSERT INTO object_locks (object_id, legal_hold_status) VALUES ('missing-version', 'ON')`)
	require.Error(t, err, "locks must reference an existing version")
	_, err = db.Exec(`INSERT INTO objects (id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at)
		VALUES ('pending-version', 'bucket', 'key', '', -1, 'PENDING', 'upload-id', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO object_locks (object_id, legal_hold_status) VALUES ('pending-version', 'ON')`)
	require.NoError(t, err, "multipart settings must be storable before completion")
	_, err = db.Exec(`INSERT INTO object_locks (object_id, legal_hold_status) VALUES ('pending-version', 'OFF')`)
	require.Error(t, err, "only one lock record per internal version ID")
	// Schema downgrade and re-upgrade must preserve the pre-existing object.
	require.NoError(t, m.Steps(-1))
	require.NoError(t, m.Migrate(39))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM objects WHERE id = 'old-version'`).Scan(&count))
	require.Equal(t, 1, count)
}
