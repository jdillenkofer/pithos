CREATE TABLE replication_progress (
 operation_id TEXT NOT NULL REFERENCES replication_operations(id) ON DELETE CASCADE,
 secondary_id TEXT NOT NULL,
 progress TEXT NOT NULL,
 PRIMARY KEY(operation_id, secondary_id)
);
