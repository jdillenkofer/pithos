ALTER TABLE authentication_credentials ADD COLUMN account_id TEXT NOT NULL DEFAULT 'legacy';
UPDATE authentication_credentials SET principal_id = access_key_id WHERE principal_id IS NULL OR principal_id = '';
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
