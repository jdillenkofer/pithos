CREATE TABLE blob_outbox_entries (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  blob_id TEXT NOT NULL,
  content BLOB NOT NULL,
  ordinal INTEGER NOT NULL UNIQUE,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);
