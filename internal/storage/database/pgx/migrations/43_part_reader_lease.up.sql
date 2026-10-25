CREATE TABLE part_reader_lease (
  id TEXT NOT NULL PRIMARY KEY,
  expires_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

CREATE TABLE part_reader_lease_part (
  lease_id TEXT NOT NULL REFERENCES part_reader_lease (id) ON DELETE CASCADE,
  part_id TEXT NOT NULL,
  PRIMARY KEY (lease_id, part_id)
);

CREATE INDEX part_reader_lease_part_part_id_idx ON part_reader_lease_part (part_id);
