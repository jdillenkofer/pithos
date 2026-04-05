ALTER TABLE objects ADD COLUMN optimistic_lock_version INTEGER NOT NULL DEFAULT 1;
