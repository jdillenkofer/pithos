
CREATE TABLE blobs2 (
  id TEXT NOT NULL primary key,
  blob_id TEXT NOT NULL,
  object_id TEXT NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  sequence_number INTEGER NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  FOREIGN KEY(object_id) REFERENCES objects(id),
  UNIQUE(object_id, sequence_number),
  UNIQUE(blob_id)
);

INSERT INTO blobs2 SELECT id, blob_id, object_id, etag, size, sequence_number, created_at, updated_at FROM blobs;
DROP TABLE blobs;

ALTER TABLE blobs2 RENAME TO blobs;

CREATE TABLE objects2 (
  id TEXT NOT NULL primary key,
  bucket_name TEXT NOT NULL,
  key TEXT NOT NULL,
  content_type TEXT NOT NULL DEFAULT "",
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  upload_status TEXT NOT NULL,
  upload_id TEXT,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  FOREIGN KEY(bucket_name) REFERENCES buckets(name)
);

INSERT INTO objects2 SELECT id, bucket_name, key, content_type, etag, size, upload_status, upload_id, created_at, updated_at FROM objects;
DROP TABLE objects;

ALTER TABLE objects2 RENAME TO objects;

CREATE UNIQUE INDEX objects_completed_unique ON objects (bucket_name, key, upload_status)
WHERE upload_status = 'COMPLETED';
CREATE UNIQUE INDEX objects_pending_unique_upload_id ON objects (bucket_name, key, upload_status, upload_id)
WHERE upload_status = 'PENDING';