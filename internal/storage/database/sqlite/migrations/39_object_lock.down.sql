DROP TABLE object_locks;
DROP TRIGGER buckets_object_lock_configuration_update;
DROP TRIGGER buckets_object_lock_configuration_insert;
ALTER TABLE buckets DROP COLUMN default_retention_years;
ALTER TABLE buckets DROP COLUMN default_retention_days;
ALTER TABLE buckets DROP COLUMN default_retention_mode;
ALTER TABLE buckets DROP COLUMN object_lock_enabled;
