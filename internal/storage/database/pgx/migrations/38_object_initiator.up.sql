CREATE TABLE object_initiators (
  object_id TEXT NOT NULL PRIMARY KEY REFERENCES objects(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  CONSTRAINT object_initiators_account_id_check CHECK (octet_length(account_id) BETWEEN 1 AND 256),
  CONSTRAINT object_initiators_principal_id_check CHECK (octet_length(principal_id) <= 256)
);