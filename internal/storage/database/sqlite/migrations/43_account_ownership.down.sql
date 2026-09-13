DROP TABLE storage_outbox_entry_create_bucket_options;
DROP TRIGGER buckets_owner_account_id_update;
DROP TRIGGER buckets_owner_account_id_insert;
DROP TRIGGER authentication_credentials_account_identity_update;
DROP TRIGGER authentication_credentials_account_identity_insert;
ALTER TABLE buckets DROP COLUMN owner_account_id;
CREATE TABLE authentication_credentials_before_account_ownership (
  access_key_id TEXT PRIMARY KEY NOT NULL,
  secret_access_key TEXT NOT NULL,
  principal_id TEXT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  version BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK (length(CAST(access_key_id AS BLOB)) BETWEEN 1 AND 128),
  CHECK (length(CAST(secret_access_key AS BLOB)) BETWEEN 1 AND 256),
  CHECK (principal_id IS NULL OR length(CAST(principal_id AS BLOB)) <= 256),
  CHECK (version >= 0)
);
INSERT INTO authentication_credentials_before_account_ownership (
  access_key_id, secret_access_key, principal_id, enabled, version, created_at, updated_at
)
SELECT access_key_id, secret_access_key, principal_id, enabled, version, created_at, updated_at
FROM authentication_credentials;
DROP TABLE authentication_credentials;
ALTER TABLE authentication_credentials_before_account_ownership RENAME TO authentication_credentials;
