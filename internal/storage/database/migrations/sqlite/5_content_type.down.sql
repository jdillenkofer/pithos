
CREATE TABLE objects2 (
  id TEXT NOT NULL primary key,
  bucket_name TEXT NOT NULL,
  key TEXT NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  upload_status TEXT NOT NULL,
  upload_id TEXT,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  FOREIGN KEY(bucket_name) REFERENCES buckets(name)
);

INSERT INTO objects2 SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at FROM objects;
DROP TABLE objects;

ALTER TABLE objects2 RENAME TO objects;
CREATE UNIQUE INDEX objects_completed_unique ON objects (bucket_name, key, upload_status)
WHERE upload_status = 'COMPLETED';
CREATE UNIQUE INDEX objects_pending_unique_upload_id ON objects (bucket_name, key, upload_status, upload_id)
WHERE upload_status = 'PENDING';

CREATE TABLE storage_outbox_entries2 (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  data BLOB NOT NULL,
  ordinal INTEGER NOT NULL UNIQUE,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);

INSERT INTO storage_outbox_entries2 SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at FROM storage_outbox_entries;
DROP TABLE storage_outbox_entries;

ALTER TABLE storage_outbox_entries2 RENAME TO storage_outbox_entries;