CREATE TABLE authentication_credentials (
    access_key_id TEXT PRIMARY KEY NOT NULL,
    secret_access_key TEXT NOT NULL,
    principal_id TEXT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    revision BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (length(CAST(access_key_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(secret_access_key AS BLOB)) BETWEEN 1 AND 256),
    CHECK (principal_id IS NULL OR length(CAST(principal_id AS BLOB)) <= 256),
    CHECK (revision > 0)
);
