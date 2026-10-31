ALTER TABLE part_registry ADD COLUMN unreferenced_at TIMESTAMP WITHOUT TIME ZONE;
ALTER TABLE part_registry ADD COLUMN orphaned BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE part_registry ADD COLUMN part_store_name TEXT;
UPDATE part_registry SET part_store_name = (SELECT MIN(p.part_store_name) FROM parts p WHERE p.part_id = part_registry.part_id);
CREATE INDEX part_registry_unreferenced_idx ON part_registry (part_id) WHERE ref_count = 0;
