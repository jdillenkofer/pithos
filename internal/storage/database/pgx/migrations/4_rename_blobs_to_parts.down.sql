ALTER TABLE parts RENAME CONSTRAINT parts_object_id_sequence_number_key TO blobs_object_id_sequence_number_key;
ALTER TABLE parts RENAME CONSTRAINT parts_object_id_fkey TO blobs_object_id_fkey;
ALTER TABLE parts RENAME CONSTRAINT parts_part_id_key TO blobs_blob_id_key;
ALTER TABLE parts RENAME CONSTRAINT parts_pkey TO blobs_pkey;
ALTER TABLE parts RENAME COLUMN part_id TO blob_id;
ALTER TABLE parts RENAME TO blobs;

ALTER TABLE part_contents RENAME CONSTRAINT part_contents_pkey TO blob_contents_pkey;
ALTER TABLE part_contents RENAME TO blob_contents;

ALTER TABLE part_outbox_entries RENAME CONSTRAINT part_outbox_entries_pkey TO blob_outbox_entries_pkey;
ALTER TABLE part_outbox_entries RENAME COLUMN part_id TO blob_id;
ALTER TABLE part_outbox_entries RENAME TO blob_outbox_entries;