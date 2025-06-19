-- Set upload_id of every completed upload to NULL
UPDATE objects
SET upload_id = NULL
WHERE upload_id != NULL AND upload_status = "COMPLETED";