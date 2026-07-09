CREATE TABLE notification_outbox_entries (
  id TEXT NOT NULL PRIMARY KEY,
  outbox_id TEXT NOT NULL DEFAULT 'default',
  destination_arn TEXT NOT NULL,
  event_name TEXT NOT NULL,
  payload_format TEXT NOT NULL,
  payload BYTEA NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  claim_owner TEXT NULL,
  claim_until TIMESTAMP WITHOUT TIME ZONE NULL,
  version INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

CREATE INDEX notification_outbox_entries_outbox_next_attempt_id_idx ON notification_outbox_entries (outbox_id, next_attempt_at, id);
CREATE INDEX notification_outbox_entries_claim_until_idx ON notification_outbox_entries (claim_until);
