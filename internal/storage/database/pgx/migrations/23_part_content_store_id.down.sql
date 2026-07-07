ALTER TABLE part_contents DROP CONSTRAINT part_contents_pkey;

ALTER TABLE part_contents ADD PRIMARY KEY (id, chunk_index);

ALTER TABLE part_contents DROP COLUMN part_store_id;
