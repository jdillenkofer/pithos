CREATE INDEX objects_completed_listing_idx
ON objects (
  bucket_name,
  key COLLATE "C",
  (COALESCE(NULLIF(version_id, 'null'), '')) COLLATE "C" DESC
)
WHERE upload_status = 'COMPLETED';
