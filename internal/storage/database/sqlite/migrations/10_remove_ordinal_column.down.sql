CREATE TABLE blob_outbox_entries2 (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  blob_id TEXT NOT NULL,
  content BLOB NOT NULL,
  ordinal INTEGER NOT NULL UNIQUE,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);

INSERT INTO blob_outbox_entries2 SELECT id, operation, blob_id, content, row_number() OVER (ORDER BY id ASC), created_at, updated_at FROM blob_outbox_entries;
DROP TABLE blob_outbox_entries;

ALTER TABLE blob_outbox_entries2 RENAME TO blob_outbox_entries;

CREATE TABLE storage_outbox_entries2 (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  data BLOB NOT NULL,
  ordinal INTEGER NOT NULL UNIQUE,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  content_type TEXT
);

INSERT INTO storage_outbox_entries2 SELECT id, operation, bucket, key, data, row_number() OVER (ORDER BY id ASC), created_at, updated_at, content_type FROM storage_outbox_entries;
DROP TABLE storage_outbox_entries;

ALTER TABLE storage_outbox_entries2 RENAME TO storage_outbox_entries;