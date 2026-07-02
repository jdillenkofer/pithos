CREATE TABLE object_tags (
  id TEXT NOT NULL PRIMARY KEY,
  object_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  FOREIGN KEY(object_id) REFERENCES objects(id),
  UNIQUE(object_id, key)
);

CREATE INDEX idx_object_tags_object_id ON object_tags(object_id);
CREATE INDEX idx_object_tags_key_value ON object_tags(key, value);
