ALTER TABLE objects ADD COLUMN optimistic_lock_version BIGINT NOT NULL DEFAULT 1;
