ALTER TABLE part_contents ADD COLUMN part_store_id TEXT NOT NULL DEFAULT 'default';

CREATE INDEX part_contents_part_store_id_id_idx ON part_contents (part_store_id, id);
