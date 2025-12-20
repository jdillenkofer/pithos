ALTER TABLE part_contents DROP CONSTRAINT part_contents_pkey;
ALTER TABLE part_contents ADD COLUMN chunk_index INTEGER NOT NULL DEFAULT 0;
ALTER TABLE part_contents ADD PRIMARY KEY (id, chunk_index);