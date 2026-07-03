DROP INDEX IF EXISTS storage_outbox_entries_outbox_id_bucket_key_id_idx;
DROP INDEX IF EXISTS storage_outbox_entries_outbox_id_bucket_id_idx;
DROP INDEX IF EXISTS storage_outbox_entries_outbox_id_id_idx;
DROP INDEX IF EXISTS part_outbox_entries_outbox_id_part_id_id_idx;
DROP INDEX IF EXISTS part_outbox_entries_outbox_id_id_idx;

ALTER TABLE storage_outbox_entries DROP COLUMN outbox_id;
ALTER TABLE part_outbox_entries DROP COLUMN outbox_id;
