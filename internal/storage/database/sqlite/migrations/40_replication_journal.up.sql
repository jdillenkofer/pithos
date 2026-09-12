CREATE TABLE replication_topologies (
 replication_id TEXT PRIMARY KEY NOT NULL,
 secondary_ids TEXT NOT NULL
);
CREATE TABLE replication_operations (
 id TEXT PRIMARY KEY NOT NULL,
 replication_id TEXT NOT NULL REFERENCES replication_topologies(replication_id),
 bucket_name TEXT NOT NULL,
 object_key TEXT NOT NULL,
 operation TEXT NOT NULL,
 payload TEXT NOT NULL,
 primary_result TEXT,
 state TEXT NOT NULL CHECK (state IN ('INTENT', 'REPLICATING', 'COMPLETE')),
 attempts INTEGER NOT NULL DEFAULT 0,
 last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX replication_operations_pending ON replication_operations(replication_id, state, id);
CREATE TABLE replication_acknowledgments (
 operation_id TEXT NOT NULL REFERENCES replication_operations(id) ON DELETE CASCADE,
 secondary_id TEXT NOT NULL,
 result TEXT NOT NULL,
 PRIMARY KEY(operation_id, secondary_id)
);
CREATE TABLE replication_mappings (
 replication_id TEXT NOT NULL REFERENCES replication_topologies(replication_id),
 secondary_id TEXT NOT NULL,
 bucket_name TEXT NOT NULL,
 object_key TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('VERSION', 'UPLOAD')),
 primary_id TEXT NOT NULL,
 secondary_object_id TEXT NOT NULL,
 PRIMARY KEY(replication_id, secondary_id, bucket_name, object_key, kind, primary_id)
);
CREATE TABLE replication_data (
 operation_id TEXT NOT NULL REFERENCES replication_operations(id) ON DELETE CASCADE,
 sequence_number INTEGER NOT NULL,
 data BLOB NOT NULL,
 PRIMARY KEY(operation_id, sequence_number)
);
