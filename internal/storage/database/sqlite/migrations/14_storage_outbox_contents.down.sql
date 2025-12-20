CREATE TABLE storage_outbox_entries_new (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  data BLOB NOT NULL DEFAULT '',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  content_type TEXT
);

INSERT INTO storage_outbox_entries_new (id, operation, bucket, key, data, created_at, updated_at, content_type)
SELECT e.id, e.operation, e.bucket, e.key, COALESCE(c.content, ''), e.created_at, e.updated_at, e.content_type
FROM storage_outbox_entries e
LEFT JOIN storage_outbox_contents c ON e.id = c.outbox_entry_id AND c.chunk_index = 0;

DROP TABLE storage_outbox_entries;
ALTER TABLE storage_outbox_entries_new RENAME TO storage_outbox_entries;

DROP TABLE storage_outbox_contents;