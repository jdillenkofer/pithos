CREATE TABLE webhook_outbox_entries (
  id TEXT NOT NULL PRIMARY KEY,
  outbox_id TEXT NOT NULL,
  url TEXT NOT NULL,
  method TEXT NOT NULL,
  headers TEXT NULL,
  body BLOB NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  claim_owner TEXT NULL,
  claim_until DATETIME NULL,
  version INTEGER NOT NULL DEFAULT 0
);
