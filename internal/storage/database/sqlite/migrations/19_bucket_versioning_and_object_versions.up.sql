ALTER TABLE buckets ADD COLUMN versioning_status TEXT;

ALTER TABLE objects ADD COLUMN version_id TEXT;
ALTER TABLE objects ADD COLUMN is_delete_marker INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects ADD COLUMN is_latest INTEGER NOT NULL DEFAULT 0;

UPDATE objects
SET version_id = 'null', is_delete_marker = 0, is_latest = 1
WHERE upload_status = 'COMPLETED';

DROP INDEX objects_completed_unique;

CREATE UNIQUE INDEX objects_completed_latest_unique ON objects (bucket_name, key, upload_status, is_latest)
WHERE upload_status = 'COMPLETED' AND is_latest = 1;

CREATE UNIQUE INDEX objects_completed_version_unique ON objects (bucket_name, key, upload_status, version_id)
WHERE upload_status = 'COMPLETED' AND version_id IS NOT NULL;
