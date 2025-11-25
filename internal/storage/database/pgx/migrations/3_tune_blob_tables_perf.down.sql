ALTER TABLE blob_contents ALTER COLUMN content SET STORAGE EXTENDED;
ALTER TABLE blob_outbox_entries ALTER COLUMN content SET STORAGE EXTENDED;
ALTER TABLE "storage_outbox_entries" ALTER COLUMN data SET STORAGE EXTENDED;

ALTER TABLE blob_contents SET (fillfactor = 100);
ALTER TABLE blob_outbox_entries SET (fillfactor = 100);
ALTER TABLE "storage_outbox_entries" SET (fillfactor = 100);