ALTER TABLE buckets ADD COLUMN versioning_status TEXT;

ALTER TABLE objects ADD COLUMN version_id TEXT;
ALTER TABLE objects ADD COLUMN is_delete_marker BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE objects ADD COLUMN is_latest BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE objects
SET version_id = 'null', is_delete_marker = FALSE, is_latest = TRUE
WHERE upload_status = 'COMPLETED';

DROP INDEX objects_completed_unique;

CREATE UNIQUE INDEX objects_completed_latest_unique ON objects (bucket_name, key, upload_status, is_latest)
WHERE upload_status = 'COMPLETED' AND is_latest = TRUE;

CREATE UNIQUE INDEX objects_completed_version_unique ON objects (bucket_name, key, upload_status, version_id)
WHERE upload_status = 'COMPLETED' AND version_id IS NOT NULL;
