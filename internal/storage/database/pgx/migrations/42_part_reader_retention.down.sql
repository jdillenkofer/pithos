DROP INDEX part_registry_ref_count_part_id_idx;
ALTER TABLE part_registry DROP COLUMN part_store_name;
ALTER TABLE part_registry DROP COLUMN orphaned;
ALTER TABLE part_registry DROP COLUMN unreferenced_at;
