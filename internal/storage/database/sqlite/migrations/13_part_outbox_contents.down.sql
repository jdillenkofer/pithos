CREATE TABLE part_outbox_entries_new (
  id TEXT NOT NULL PRIMARY KEY,
  operation TEXT NOT NULL,
  part_id TEXT NOT NULL,
  content BLOB NOT NULL DEFAULT '',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);

INSERT INTO part_outbox_entries_new (id, operation, part_id, content, created_at, updated_at)
SELECT e.id, e.operation, e.part_id, COALESCE(c.content, ''), e.created_at, e.updated_at
FROM part_outbox_entries e
LEFT JOIN part_outbox_contents c ON e.id = c.outbox_entry_id AND c.chunk_index = 0;

DROP TABLE part_outbox_entries;
ALTER TABLE part_outbox_entries_new RENAME TO part_outbox_entries;

DROP TABLE part_outbox_contents;
