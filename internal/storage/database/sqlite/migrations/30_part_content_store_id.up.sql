CREATE TABLE part_contents_new (
  id TEXT NOT NULL,
  part_store_id TEXT NOT NULL DEFAULT 'default',
  content BLOB NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  chunk_index INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (part_store_id, id, chunk_index)
);

INSERT INTO part_contents_new (id, part_store_id, content, created_at, updated_at, chunk_index)
SELECT id, 'default', content, created_at, updated_at, chunk_index FROM part_contents;

DROP TABLE part_contents;
ALTER TABLE part_contents_new RENAME TO part_contents;
