CREATE TABLE part_registry (
  part_id TEXT NOT NULL PRIMARY KEY,
  ref_count INTEGER NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);

INSERT INTO part_registry (part_id, ref_count, created_at, updated_at)
SELECT part_id, COUNT(*), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM parts
GROUP BY part_id;
