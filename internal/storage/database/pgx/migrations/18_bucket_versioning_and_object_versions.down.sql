DROP INDEX IF EXISTS objects_completed_version_unique;
DROP INDEX IF EXISTS objects_completed_latest_unique;

CREATE UNIQUE INDEX objects_completed_unique ON objects (bucket_name, key, upload_status)
WHERE upload_status = 'COMPLETED';

ALTER TABLE objects DROP COLUMN is_latest;
ALTER TABLE objects DROP COLUMN is_delete_marker;
ALTER TABLE objects DROP COLUMN version_id;
ALTER TABLE buckets DROP COLUMN versioning_status;
