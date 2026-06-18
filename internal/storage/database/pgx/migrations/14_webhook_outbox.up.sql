CREATE TABLE webhook_outbox_entries (
  id TEXT NOT NULL PRIMARY KEY,
  outbox_id TEXT NOT NULL,
  url TEXT NOT NULL,
  method TEXT NOT NULL,
  headers TEXT NULL,
  body BYTEA NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  claim_owner TEXT NULL,
  claim_until TIMESTAMP NULL,
  version INTEGER NOT NULL DEFAULT 0
);
