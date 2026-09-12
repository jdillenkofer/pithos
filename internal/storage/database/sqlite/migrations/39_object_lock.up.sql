ALTER TABLE buckets ADD COLUMN object_lock_enabled BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE buckets ADD COLUMN default_retention_mode TEXT;
ALTER TABLE buckets ADD COLUMN default_retention_days INTEGER;
ALTER TABLE buckets ADD COLUMN default_retention_years INTEGER;

CREATE TRIGGER buckets_object_lock_configuration_insert
BEFORE INSERT ON buckets WHEN (
  (NEW.object_lock_enabled = 0 AND NEW.default_retention_mode IS NULL AND NEW.default_retention_days IS NULL AND NEW.default_retention_years IS NULL)
  OR (NEW.object_lock_enabled = 1 AND (
    (NEW.default_retention_mode IS NULL AND NEW.default_retention_days IS NULL AND NEW.default_retention_years IS NULL)
    OR (NEW.default_retention_mode IN ('GOVERNANCE', 'COMPLIANCE') AND
      ((NEW.default_retention_days > 0 AND NEW.default_retention_years IS NULL)
       OR (NEW.default_retention_years > 0 AND NEW.default_retention_days IS NULL)))
  ))
) IS NOT TRUE BEGIN SELECT RAISE(ABORT, 'invalid object lock configuration'); END;

CREATE TRIGGER buckets_object_lock_configuration_update
BEFORE UPDATE OF object_lock_enabled, default_retention_mode, default_retention_days, default_retention_years ON buckets
WHEN (
  (NEW.object_lock_enabled = 0 AND NEW.default_retention_mode IS NULL AND NEW.default_retention_days IS NULL AND NEW.default_retention_years IS NULL)
  OR (NEW.object_lock_enabled = 1 AND (
    (NEW.default_retention_mode IS NULL AND NEW.default_retention_days IS NULL AND NEW.default_retention_years IS NULL)
    OR (NEW.default_retention_mode IN ('GOVERNANCE', 'COMPLIANCE') AND
      ((NEW.default_retention_days > 0 AND NEW.default_retention_years IS NULL)
       OR (NEW.default_retention_years > 0 AND NEW.default_retention_days IS NULL)))
  ))
) IS NOT TRUE BEGIN SELECT RAISE(ABORT, 'invalid object lock configuration'); END;

-- The object ID identifies a concrete version, including a pending multipart
-- upload. No rows are backfilled: existing versions remain unprotected.
CREATE TABLE object_locks (
  object_id TEXT NOT NULL PRIMARY KEY REFERENCES objects(id) ON DELETE CASCADE,
  retention_mode TEXT,
  retain_until_date DATETIME,
  legal_hold_status TEXT CHECK (legal_hold_status IN ('ON', 'OFF')),
  CHECK (
    (retention_mode IS NULL AND retain_until_date IS NULL)
    OR (retention_mode IS NOT NULL AND retain_until_date IS NOT NULL AND retention_mode IN ('GOVERNANCE', 'COMPLIANCE'))
  )
);
