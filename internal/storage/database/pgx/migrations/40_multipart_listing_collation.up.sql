CREATE INDEX objects_pending_listing_idx
ON objects (bucket_name, key COLLATE "C", upload_id COLLATE "C")
WHERE upload_status = 'PENDING';
