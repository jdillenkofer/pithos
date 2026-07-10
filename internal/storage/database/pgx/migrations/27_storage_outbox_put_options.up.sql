ALTER TABLE storage_outbox_entries ADD COLUMN storage_class TEXT;
ALTER TABLE storage_outbox_entries ADD COLUMN cache_control TEXT;
ALTER TABLE storage_outbox_entries ADD COLUMN content_disposition TEXT;
ALTER TABLE storage_outbox_entries ADD COLUMN content_encoding TEXT;
ALTER TABLE storage_outbox_entries ADD COLUMN content_language TEXT;
ALTER TABLE storage_outbox_entries ADD COLUMN expires TEXT;
ALTER TABLE storage_outbox_entries ADD COLUMN website_redirect_location TEXT;

CREATE TABLE storage_outbox_entry_tags (
  outbox_entry_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (outbox_entry_id, key),
  FOREIGN KEY (outbox_entry_id) REFERENCES storage_outbox_entries(id) ON DELETE CASCADE
);

CREATE TABLE storage_outbox_entry_user_metadata (
  outbox_entry_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (outbox_entry_id, key),
  FOREIGN KEY (outbox_entry_id) REFERENCES storage_outbox_entries(id) ON DELETE CASCADE
);
