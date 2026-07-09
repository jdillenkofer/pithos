ALTER TABLE notification_outbox_entries ADD COLUMN last_error TEXT NULL;
ALTER TABLE notification_outbox_entries ADD COLUMN last_error_at TIMESTAMP WITHOUT TIME ZONE NULL;
ALTER TABLE notification_outbox_entries ADD COLUMN dead_lettered_at TIMESTAMP WITHOUT TIME ZONE NULL;

CREATE INDEX notification_outbox_entries_dead_lettered_at_idx ON notification_outbox_entries (dead_lettered_at);
