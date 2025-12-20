ALTER TABLE storage_outbox_entries ADD COLUMN data BYTEA NOT NULL DEFAULT ''::bytea;

-- Reassemble chunks (taking only chunk 0 for simplicity during downgrade)
UPDATE storage_outbox_entries
SET data = (SELECT content FROM storage_outbox_contents WHERE outbox_entry_id = storage_outbox_entries.id AND chunk_index = 0)
WHERE id IN (SELECT outbox_entry_id FROM storage_outbox_contents WHERE chunk_index = 0);

DROP TABLE storage_outbox_contents;
