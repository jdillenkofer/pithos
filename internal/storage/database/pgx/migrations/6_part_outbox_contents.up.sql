CREATE TABLE part_outbox_contents (
  outbox_entry_id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  content BYTEA NOT NULL,
  PRIMARY KEY (outbox_entry_id, chunk_index),
  FOREIGN KEY (outbox_entry_id) REFERENCES part_outbox_entries(id) ON DELETE CASCADE
);

-- Migrate existing data as chunk 0 where content is not empty
INSERT INTO part_outbox_contents (outbox_entry_id, chunk_index, content)
SELECT id, 0, content FROM part_outbox_entries WHERE length(content) > 0;

-- Remove content from metadata table
ALTER TABLE part_outbox_entries DROP COLUMN content;
