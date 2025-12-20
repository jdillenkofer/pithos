ALTER TABLE parts RENAME COLUMN part_id TO blob_id;
ALTER TABLE parts RENAME TO blobs;

ALTER TABLE part_contents RENAME TO blob_contents;

ALTER TABLE part_outbox_entries RENAME COLUMN part_id TO blob_id;
ALTER TABLE part_outbox_entries RENAME TO blob_outbox_entries;
