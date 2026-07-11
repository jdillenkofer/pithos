CREATE TABLE part_dedup_index (
  part_store_name TEXT NOT NULL,
  checksum_sha256 TEXT NOT NULL,
  size BIGINT NOT NULL,
  etag TEXT NOT NULL,
  checksum_crc32 TEXT NOT NULL,
  checksum_crc32c TEXT NOT NULL,
  checksum_crc64nvme TEXT NOT NULL,
  checksum_sha1 TEXT NOT NULL,
  part_id TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  PRIMARY KEY (part_store_name, checksum_sha256, size)
);

CREATE INDEX part_dedup_index_part_id_idx ON part_dedup_index (part_id);
