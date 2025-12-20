CREATE TABLE storage_outbox_contents (
  outbox_entry_id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  content BLOB NOT NULL,
  PRIMARY KEY (outbox_entry_id, chunk_index),
  FOREIGN KEY (outbox_entry_id) REFERENCES storage_outbox_entries(id) ON DELETE CASCADE
);

INSERT INTO storage_outbox_contents (outbox_entry_id, chunk_index, content)
SELECT id, 0, data FROM storage_outbox_entries WHERE length(data) > 0;

CREATE TABLE storage_outbox_entries_new (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  content_type TEXT
);

INSERT INTO storage_outbox_entries_new (id, operation, bucket, key, created_at, updated_at, content_type)
SELECT id, operation, bucket, key, created_at, updated_at, content_type FROM storage_outbox_entries;

DROP TABLE storage_outbox_entries;
ALTER TABLE storage_outbox_entries_new RENAME TO storage_outbox_entries;