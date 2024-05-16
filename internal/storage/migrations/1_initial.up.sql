CREATE TABLE buckets (
  id TEXT NOT NULL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);

CREATE TABLE objects (
  id TEXT NOT NULL primary key,
  bucket_name TEXT NOT NULL,
  key TEXT NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  FOREIGN KEY(bucket_name) REFERENCES buckets(name),
  UNIQUE(bucket_name, key)
);

CREATE TABLE blobs (
  id TEXT NOT NULL primary key,
  object_id TEXT NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  sequence_number INTEGER NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  FOREIGN KEY(object_id) REFERENCES objects(id),
  UNIQUE(object_id, sequence_number)
);
