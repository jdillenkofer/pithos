ALTER TABLE authentication_credentials ADD COLUMN account_id TEXT NOT NULL DEFAULT 'legacy';
UPDATE authentication_credentials SET principal_id = access_key_id WHERE principal_id IS NULL OR principal_id = '';
CREATE TABLE authentication_credentials_account_ownership (
  access_key_id TEXT PRIMARY KEY NOT NULL,
  secret_access_key TEXT NOT NULL,
  account_id TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  version BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK (length(CAST(access_key_id AS BLOB)) BETWEEN 1 AND 128),
  CHECK (length(CAST(secret_access_key AS BLOB)) BETWEEN 1 AND 256),
  CHECK (length(CAST(account_id AS BLOB)) BETWEEN 1 AND 256),
  CHECK (length(CAST(principal_id AS BLOB)) BETWEEN 1 AND 256),
  CHECK (version >= 0)
);
INSERT INTO authentication_credentials_account_ownership (
  access_key_id, secret_access_key, account_id, principal_id, enabled, version, created_at, updated_at
)
SELECT access_key_id, secret_access_key, account_id, principal_id, enabled, version, created_at, updated_at
FROM authentication_credentials;
DROP TABLE authentication_credentials;
ALTER TABLE authentication_credentials_account_ownership RENAME TO authentication_credentials;
ALTER TABLE buckets ADD COLUMN owner_account_id TEXT NOT NULL DEFAULT 'legacy';
CREATE TRIGGER authentication_credentials_account_identity_insert
BEFORE INSERT ON authentication_credentials WHEN (
  NEW.account_id IS NULL OR length(CAST(NEW.account_id AS BLOB)) NOT BETWEEN 1 AND 256 OR
  NEW.principal_id IS NULL OR length(CAST(NEW.principal_id AS BLOB)) NOT BETWEEN 1 AND 256
) BEGIN SELECT RAISE(ABORT, 'invalid account or principal ID'); END;
CREATE TRIGGER authentication_credentials_account_identity_update
BEFORE UPDATE OF account_id, principal_id ON authentication_credentials WHEN (
  NEW.account_id IS NULL OR length(CAST(NEW.account_id AS BLOB)) NOT BETWEEN 1 AND 256 OR
  NEW.principal_id IS NULL OR length(CAST(NEW.principal_id AS BLOB)) NOT BETWEEN 1 AND 256
) BEGIN SELECT RAISE(ABORT, 'invalid account or principal ID'); END;
CREATE TRIGGER buckets_owner_account_id_insert
BEFORE INSERT ON buckets WHEN length(CAST(NEW.owner_account_id AS BLOB)) NOT BETWEEN 1 AND 256
BEGIN SELECT RAISE(ABORT, 'invalid owner account ID'); END;
CREATE TRIGGER buckets_owner_account_id_update
BEFORE UPDATE OF owner_account_id ON buckets WHEN length(CAST(NEW.owner_account_id AS BLOB)) NOT BETWEEN 1 AND 256
BEGIN SELECT RAISE(ABORT, 'invalid owner account ID'); END;
CREATE TABLE storage_outbox_entry_create_bucket_options (
  outbox_entry_id TEXT PRIMARY KEY,
  owner_account_id TEXT NOT NULL,
  FOREIGN KEY(outbox_entry_id) REFERENCES storage_outbox_entries(id) ON DELETE CASCADE
);
INSERT INTO storage_outbox_entry_create_bucket_options (outbox_entry_id, owner_account_id)
SELECT id, 'legacy' FROM storage_outbox_entries WHERE operation = 'CreateBucket';
