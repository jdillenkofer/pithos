ALTER TABLE parts DROP CONSTRAINT parts_part_id_key;
CREATE INDEX parts_part_id_idx ON parts (part_id);
