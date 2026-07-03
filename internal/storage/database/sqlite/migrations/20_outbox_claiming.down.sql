ALTER TABLE storage_outbox_entries DROP COLUMN version;
ALTER TABLE storage_outbox_entries DROP COLUMN claim_until;
ALTER TABLE storage_outbox_entries DROP COLUMN claim_owner;

ALTER TABLE part_outbox_entries DROP COLUMN version;
ALTER TABLE part_outbox_entries DROP COLUMN claim_until;
ALTER TABLE part_outbox_entries DROP COLUMN claim_owner;
