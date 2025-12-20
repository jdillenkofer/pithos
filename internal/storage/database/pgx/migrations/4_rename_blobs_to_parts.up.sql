ALTER TABLE blobs RENAME TO parts;
ALTER TABLE parts RENAME COLUMN blob_id TO part_id;
ALTER TABLE parts RENAME CONSTRAINT blobs_pkey TO parts_pkey;
ALTER TABLE parts RENAME CONSTRAINT blobs_blob_id_key TO parts_part_id_key;
ALTER TABLE parts RENAME CONSTRAINT blobs_object_id_fkey TO parts_object_id_fkey;
ALTER TABLE parts RENAME CONSTRAINT blobs_object_id_sequence_number_key TO parts_object_id_sequence_number_key;

ALTER TABLE blob_contents RENAME TO part_contents;
ALTER TABLE part_contents RENAME CONSTRAINT blob_contents_pkey TO part_contents_pkey;

ALTER TABLE blob_outbox_entries RENAME TO part_outbox_entries;
ALTER TABLE part_outbox_entries RENAME COLUMN blob_id TO part_id;
ALTER TABLE part_outbox_entries RENAME CONSTRAINT blob_outbox_entries_pkey TO part_outbox_entries_pkey;