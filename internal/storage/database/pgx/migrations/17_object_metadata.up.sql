ALTER TABLE objects ADD COLUMN cache_control TEXT;
ALTER TABLE objects ADD COLUMN content_disposition TEXT;
ALTER TABLE objects ADD COLUMN content_encoding TEXT;
ALTER TABLE objects ADD COLUMN content_language TEXT;
ALTER TABLE objects ADD COLUMN expires TEXT;
ALTER TABLE objects ADD COLUMN website_redirect_location TEXT;

CREATE TABLE object_user_metadata (
  id TEXT NOT NULL PRIMARY KEY,
  object_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  FOREIGN KEY(object_id) REFERENCES objects(id),
  UNIQUE(object_id, key)
);

CREATE INDEX idx_object_user_metadata_object_id ON object_user_metadata(object_id);
