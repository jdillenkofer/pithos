CREATE TABLE part_contents_new (
  id TEXT NOT NULL,
  content BLOB NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  chunk_index INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (id, chunk_index)
);

INSERT INTO part_contents_new (id, content, created_at, updated_at, chunk_index)
SELECT id, content, created_at, updated_at, chunk_index FROM part_contents;

DROP TABLE part_contents;
ALTER TABLE part_contents_new RENAME TO part_contents;
