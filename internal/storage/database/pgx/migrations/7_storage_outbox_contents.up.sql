CREATE TABLE storage_outbox_contents (
  outbox_entry_id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  content BYTEA NOT NULL,
  PRIMARY KEY (outbox_entry_id, chunk_index),
  FOREIGN KEY (outbox_entry_id) REFERENCES storage_outbox_entries(id) ON DELETE CASCADE
);

-- Migrate existing data as chunk 0
INSERT INTO storage_outbox_contents (outbox_entry_id, chunk_index, content)
SELECT id, 0, data FROM storage_outbox_entries WHERE length(data) > 0;

-- Remove data from metadata table
ALTER TABLE storage_outbox_entries DROP COLUMN data;
