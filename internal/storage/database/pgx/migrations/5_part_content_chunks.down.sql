ALTER TABLE part_contents DROP CONSTRAINT part_contents_pkey;
DELETE FROM part_contents WHERE chunk_index > 0;
ALTER TABLE part_contents DROP COLUMN chunk_index;
ALTER TABLE part_contents ADD PRIMARY KEY (id);