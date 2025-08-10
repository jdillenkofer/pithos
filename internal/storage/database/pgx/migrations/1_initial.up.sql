CREATE TABLE buckets (
  id TEXT NOT NULL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE blob_contents (
  id TEXT NOT NULL primary key,
  content BYTEA NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE blob_outbox_entries (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  blob_id TEXT NOT NULL,
  content BYTEA NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE "objects" (
  id TEXT NOT NULL primary key,
  bucket_name TEXT NOT NULL,
  key TEXT NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  upload_status TEXT NOT NULL,
  upload_id TEXT,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  content_type TEXT,
  checksum_crc32 TEXT,
  checksum_crc32c TEXT,
  checksum_crc64nvme TEXT,
  checksum_sha1 TEXT,
  checksum_sha256 TEXT,
  checksum_type TEXT,
  FOREIGN KEY(bucket_name) REFERENCES buckets(name)
);

CREATE TABLE blobs (
  id TEXT NOT NULL primary key,
  blob_id TEXT NOT NULL,
  object_id TEXT NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  sequence_number INTEGER NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  checksum_crc32 TEXT,
  checksum_crc32c TEXT,
  checksum_crc64nvme TEXT,
  checksum_sha1 TEXT,
  checksum_sha256 TEXT,
  FOREIGN KEY(object_id) REFERENCES objects(id),
  UNIQUE(object_id, sequence_number),
  UNIQUE(blob_id)
);

CREATE UNIQUE INDEX objects_completed_unique ON objects (bucket_name, key, upload_status)
WHERE upload_status = 'COMPLETED';

CREATE UNIQUE INDEX objects_pending_unique_upload_id ON objects (bucket_name, key, upload_status, upload_id)
WHERE upload_status = 'PENDING';

CREATE TABLE "storage_outbox_entries" (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  data BYTEA NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  content_type TEXT
);