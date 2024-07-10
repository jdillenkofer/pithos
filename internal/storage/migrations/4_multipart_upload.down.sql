
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

INSERT INTO objects2 SELECT * FROM objects;
DROP INDEX objects_completed_unique;
DROP INDEX objects_pending_or_aborted_unique_upload_id;
DROP TABLE objects;

ALTER TABLE objects2 RENAME TO objects;

