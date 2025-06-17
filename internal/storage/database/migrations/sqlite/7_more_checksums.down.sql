
CREATE TABLE blobs2 (
  id TEXT NOT NULL primary key,
  blob_id TEXT NOT NULL,
  object_id TEXT NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  sequence_number INTEGER NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  FOREIGN KEY(object_id) REFERENCES objects(id),
  UNIQUE(object_id, sequence_number),
  UNIQUE(blob_id)
);

INSERT INTO blobs2 SELECT id, blob_id, object_id, etag, size, sequence_number, created_at, updated_at FROM blobs;
DROP TABLE blobs;

ALTER TABLE blobs2 RENAME TO blobs;