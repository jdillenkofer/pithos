ALTER TABLE buckets
  ADD COLUMN object_lock_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN default_retention_mode TEXT,
  ADD COLUMN default_retention_days INTEGER,
  ADD COLUMN default_retention_years INTEGER,
  ADD CONSTRAINT buckets_object_lock_configuration_check CHECK (
    (object_lock_enabled = FALSE AND default_retention_mode IS NULL AND default_retention_days IS NULL AND default_retention_years IS NULL)
    OR (object_lock_enabled = TRUE AND (
      (default_retention_mode IS NULL AND default_retention_days IS NULL AND default_retention_years IS NULL)
      OR (default_retention_mode IN ('GOVERNANCE', 'COMPLIANCE') AND
        ((default_retention_days IS NOT NULL AND default_retention_days > 0 AND default_retention_years IS NULL)
         OR (default_retention_years IS NOT NULL AND default_retention_years > 0 AND default_retention_days IS NULL)))
    ))
  );

-- The object ID identifies a concrete version, including a pending multipart
-- upload. No rows are backfilled: existing versions remain unprotected.
CREATE TABLE object_locks (
  object_id TEXT NOT NULL PRIMARY KEY REFERENCES objects(id) ON DELETE CASCADE,
  retention_mode TEXT,
  retain_until_date TIMESTAMP WITH TIME ZONE,
  legal_hold_status TEXT CHECK (legal_hold_status IN ('ON', 'OFF')),
  CHECK (
    (retention_mode IS NULL AND retain_until_date IS NULL)
    OR (retention_mode IS NOT NULL AND retain_until_date IS NOT NULL AND retention_mode IN ('GOVERNANCE', 'COMPLIANCE'))
  )
);
