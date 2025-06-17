ALTER TABLE blobs ADD COLUMN checksum_crc32 TEXT;
ALTER TABLE blobs ADD COLUMN checksum_crc32c TEXT;
ALTER TABLE blobs ADD COLUMN checksum_crc64nvme TEXT;
ALTER TABLE blobs ADD COLUMN checksum_sha1 TEXT;
ALTER TABLE blobs ADD COLUMN checksum_sha256 TEXT;