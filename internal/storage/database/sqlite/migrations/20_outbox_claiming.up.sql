ALTER TABLE storage_outbox_entries ADD COLUMN claim_owner TEXT NULL;
ALTER TABLE storage_outbox_entries ADD COLUMN claim_until DATETIME NULL;
ALTER TABLE storage_outbox_entries ADD COLUMN version INTEGER NOT NULL DEFAULT 0;

ALTER TABLE part_outbox_entries ADD COLUMN claim_owner TEXT NULL;
ALTER TABLE part_outbox_entries ADD COLUMN claim_until DATETIME NULL;
ALTER TABLE part_outbox_entries ADD COLUMN version INTEGER NOT NULL DEFAULT 0;
