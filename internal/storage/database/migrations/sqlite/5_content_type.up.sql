ALTER TABLE storage_outbox_entries ADD COLUMN content_type TEXT NOT NULL DEFAULT "";
ALTER TABLE objects ADD COLUMN content_type TEXT NOT NULL DEFAULT "";