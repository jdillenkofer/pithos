ALTER TABLE part_registry ADD COLUMN unreferenced_at TIMESTAMP WITHOUT TIME ZONE;
UPDATE part_registry SET unreferenced_at = CURRENT_TIMESTAMP WHERE ref_count = 0;
