DROP TABLE storage_outbox_entry_user_metadata;
DROP TABLE storage_outbox_entry_tags;

ALTER TABLE storage_outbox_entries DROP COLUMN website_redirect_location;
ALTER TABLE storage_outbox_entries DROP COLUMN expires;
ALTER TABLE storage_outbox_entries DROP COLUMN content_language;
ALTER TABLE storage_outbox_entries DROP COLUMN content_encoding;
ALTER TABLE storage_outbox_entries DROP COLUMN content_disposition;
ALTER TABLE storage_outbox_entries DROP COLUMN cache_control;
ALTER TABLE storage_outbox_entries DROP COLUMN storage_class;
