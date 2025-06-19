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
  content_type TEXT NOT NULL,
  checksum_crc32 TEXT,
  checksum_crc32c TEXT,
  checksum_crc64nvme TEXT,
  checksum_sha1 TEXT,
  checksum_sha256 TEXT,
  checksum_type TEXT,
  FOREIGN KEY(bucket_name) REFERENCES buckets(name)
);

INSERT INTO objects2 SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at, content_type, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type FROM objects WHERE content_type IS NOT NULL;
INSERT INTO objects2 SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at, '', checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type FROM objects WHERE content_type IS NULL;
DROP TABLE objects;

ALTER TABLE objects2 RENAME TO objects;

CREATE TABLE storage_outbox_entries2 (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  data BLOB NOT NULL,
  ordinal INTEGER NOT NULL UNIQUE,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  content_type TEXT NOT NULL DEFAULT ""
);

INSERT INTO storage_outbox_entries2 SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at, content_type FROM storage_outbox_entries WHERE content_type IS NOT NULL;
INSERT INTO storage_outbox_entries2 SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at, '' FROM storage_outbox_entries WHERE content_type IS NULL;
DROP TABLE storage_outbox_entries;

ALTER TABLE storage_outbox_entries2 RENAME TO storage_outbox_entries;