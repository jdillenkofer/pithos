
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
  FOREIGN KEY(bucket_name) REFERENCES buckets(name),
  UNIQUE(bucket_name, key)
);

INSERT INTO objects2 SELECT * FROM objects;
DROP TABLE objects;

ALTER TABLE objects2 RENAME TO objects;
CREATE UNIQUE INDEX objects_completed_unique ON objects (bucket_name, key, upload_status) WHERE upload_status = 'COMPLETED';
CREATE UNIQUE INDEX objects_pending_unique_upload_id ON objects (bucket_name, key, upload_status, upload_id)
WHERE upload_status = 'PENDING';
