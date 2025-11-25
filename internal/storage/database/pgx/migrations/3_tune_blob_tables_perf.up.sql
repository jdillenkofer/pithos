ALTER TABLE blob_contents ALTER COLUMN content SET STORAGE EXTERNAL;
ALTER TABLE blob_outbox_entries ALTER COLUMN content SET STORAGE EXTERNAL;
ALTER TABLE "storage_outbox_entries" ALTER COLUMN data SET STORAGE EXTERNAL;

ALTER TABLE blob_contents SET (fillfactor = 70);
ALTER TABLE blob_outbox_entries SET (fillfactor = 70);
ALTER TABLE "storage_outbox_entries" SET (fillfactor = 70);