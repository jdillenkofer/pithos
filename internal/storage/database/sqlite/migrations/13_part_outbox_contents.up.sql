CREATE TABLE part_outbox_contents (
  outbox_entry_id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  content BLOB NOT NULL,
  PRIMARY KEY (outbox_entry_id, chunk_index),
  FOREIGN KEY (outbox_entry_id) REFERENCES part_outbox_entries(id) ON DELETE CASCADE
);

INSERT INTO part_outbox_contents (outbox_entry_id, chunk_index, content)
SELECT id, 0, content FROM part_outbox_entries WHERE length(content) > 0;

CREATE TABLE part_outbox_entries_new (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  part_id TEXT NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);

INSERT INTO part_outbox_entries_new (id, operation, part_id, created_at, updated_at)
SELECT id, operation, part_id, created_at, updated_at FROM part_outbox_entries;

DROP TABLE part_outbox_entries;
ALTER TABLE part_outbox_entries_new RENAME TO part_outbox_entries;
