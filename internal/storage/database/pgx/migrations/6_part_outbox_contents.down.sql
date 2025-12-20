ALTER TABLE part_outbox_entries ADD COLUMN content BYTEA NOT NULL DEFAULT ''::bytea;

-- Reassemble chunks (taking only chunk 0 for simplicity during downgrade)
UPDATE part_outbox_entries
SET content = (SELECT content FROM part_outbox_contents WHERE outbox_entry_id = part_outbox_entries.id AND chunk_index = 0)
WHERE id IN (SELECT outbox_entry_id FROM part_outbox_contents WHERE chunk_index = 0);

DROP TABLE part_outbox_contents;
