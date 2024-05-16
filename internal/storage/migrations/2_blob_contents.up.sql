CREATE TABLE blob_contents (
  id TEXT NOT NULL primary key,
  content BLOB NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);
