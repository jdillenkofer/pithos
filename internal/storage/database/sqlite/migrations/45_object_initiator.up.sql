CREATE TABLE object_initiators (
  object_id TEXT NOT NULL PRIMARY KEY,
  account_id TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  FOREIGN KEY(object_id) REFERENCES objects(id) ON DELETE CASCADE,
  CHECK (length(CAST(account_id AS BLOB)) BETWEEN 1 AND 256),
  CHECK (length(CAST(principal_id AS BLOB)) <= 256)
);