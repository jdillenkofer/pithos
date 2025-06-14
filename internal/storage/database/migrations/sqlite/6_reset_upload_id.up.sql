-- Set upload_id of every completed upload to empty string
UPDATE objects
SET upload_id = ""
WHERE upload_id != "" AND upload_status = "COMPLETED";