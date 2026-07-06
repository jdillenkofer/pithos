DROP INDEX IF EXISTS part_contents_part_store_id_id_idx;

ALTER TABLE part_contents DROP COLUMN part_store_id;
