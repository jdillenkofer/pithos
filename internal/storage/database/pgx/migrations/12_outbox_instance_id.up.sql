ALTER TABLE part_outbox_entries ADD COLUMN outbox_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE storage_outbox_entries ADD COLUMN outbox_id TEXT NOT NULL DEFAULT 'default';

CREATE INDEX part_outbox_entries_outbox_id_id_idx ON part_outbox_entries (outbox_id, id);
CREATE INDEX part_outbox_entries_outbox_id_part_id_id_idx ON part_outbox_entries (outbox_id, part_id, id);
CREATE INDEX storage_outbox_entries_outbox_id_id_idx ON storage_outbox_entries (outbox_id, id);
CREATE INDEX storage_outbox_entries_outbox_id_bucket_id_idx ON storage_outbox_entries (outbox_id, bucket, id);
CREATE INDEX storage_outbox_entries_outbox_id_bucket_key_id_idx ON storage_outbox_entries (outbox_id, bucket, key, id);
