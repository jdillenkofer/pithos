DROP INDEX parts_part_id_idx;
-- This intentionally fails if shared references still exist.
ALTER TABLE parts ADD CONSTRAINT parts_part_id_key UNIQUE (part_id);
