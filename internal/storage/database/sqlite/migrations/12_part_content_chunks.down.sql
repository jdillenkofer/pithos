CREATE TABLE part_contents_new (
  id TEXT NOT NULL PRIMARY KEY,
  content BLOB NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);
INSERT INTO part_contents_new (id, content, created_at, updated_at) 
SELECT id, content, created_at, updated_at FROM part_contents WHERE chunk_index = 0;
DROP TABLE part_contents;
ALTER TABLE part_contents_new RENAME TO part_contents;