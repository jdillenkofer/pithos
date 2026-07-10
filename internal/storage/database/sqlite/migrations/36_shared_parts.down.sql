CREATE TABLE parts_new (
  id TEXT NOT NULL PRIMARY KEY,
  part_id TEXT NOT NULL,
  object_id TEXT NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  sequence_number INTEGER NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  checksum_crc32 TEXT,
  checksum_crc32c TEXT,
  checksum_crc64nvme TEXT,
  checksum_sha1 TEXT,
  checksum_sha256 TEXT,
  part_store_name TEXT,
  FOREIGN KEY(object_id) REFERENCES objects(id),
  UNIQUE(object_id, sequence_number),
  UNIQUE(part_id)
);

-- This copy intentionally fails if shared references still exist.
INSERT INTO parts_new (id, part_id, object_id, etag, size, sequence_number, created_at, updated_at, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, part_store_name)
SELECT id, part_id, object_id, etag, size, sequence_number, created_at, updated_at, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, part_store_name FROM parts;

DROP TABLE parts;
ALTER TABLE parts_new RENAME TO parts;
