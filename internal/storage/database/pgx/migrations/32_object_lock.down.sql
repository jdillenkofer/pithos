DROP TABLE object_locks;
ALTER TABLE buckets DROP CONSTRAINT buckets_object_lock_configuration_check,
  DROP COLUMN default_retention_years, DROP COLUMN default_retention_days,
  DROP COLUMN default_retention_mode, DROP COLUMN object_lock_enabled;
