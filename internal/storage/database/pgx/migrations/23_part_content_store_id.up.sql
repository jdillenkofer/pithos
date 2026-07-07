ALTER TABLE part_contents ADD COLUMN part_store_id TEXT NOT NULL DEFAULT 'default';

ALTER TABLE part_contents DROP CONSTRAINT part_contents_pkey;

ALTER TABLE part_contents ADD PRIMARY KEY (part_store_id, id, chunk_index);
