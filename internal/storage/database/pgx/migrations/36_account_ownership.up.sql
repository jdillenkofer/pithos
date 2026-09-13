ALTER TABLE authentication_credentials ADD COLUMN account_id TEXT;
UPDATE authentication_credentials SET account_id = 'legacy';
UPDATE authentication_credentials SET principal_id = access_key_id WHERE principal_id IS NULL OR principal_id = '';
ALTER TABLE authentication_credentials ALTER COLUMN account_id SET NOT NULL;
ALTER TABLE authentication_credentials ALTER COLUMN principal_id SET NOT NULL;
ALTER TABLE buckets ADD COLUMN owner_account_id TEXT;
UPDATE buckets SET owner_account_id = 'legacy';
ALTER TABLE buckets ALTER COLUMN owner_account_id SET NOT NULL;
ALTER TABLE authentication_credentials ADD CONSTRAINT authentication_credentials_account_id_check CHECK (octet_length(account_id) BETWEEN 1 AND 256);
ALTER TABLE authentication_credentials ADD CONSTRAINT authentication_credentials_principal_required_check CHECK (octet_length(principal_id) BETWEEN 1 AND 256);
ALTER TABLE buckets ADD CONSTRAINT buckets_owner_account_id_check CHECK (octet_length(owner_account_id) BETWEEN 1 AND 256);
CREATE TABLE storage_outbox_entry_create_bucket_options (
  outbox_entry_id TEXT PRIMARY KEY REFERENCES storage_outbox_entries(id) ON DELETE CASCADE,
  owner_account_id TEXT NOT NULL
);
