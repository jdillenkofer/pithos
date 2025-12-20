ALTER TABLE blobs RENAME TO parts;
ALTER TABLE parts RENAME COLUMN blob_id TO part_id;

ALTER TABLE blob_contents RENAME TO part_contents;

ALTER TABLE blob_outbox_entries RENAME TO part_outbox_entries;
ALTER TABLE part_outbox_entries RENAME COLUMN blob_id TO part_id;
