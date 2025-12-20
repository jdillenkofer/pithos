CREATE TABLE part_contents_new (
  id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL DEFAULT 0,
  content BLOB NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (id, chunk_index)
);
INSERT INTO part_contents_new (id, content, created_at, updated_at) SELECT id, content, created_at, updated_at FROM part_contents;
DROP TABLE part_contents;
ALTER TABLE part_contents_new RENAME TO part_contents;