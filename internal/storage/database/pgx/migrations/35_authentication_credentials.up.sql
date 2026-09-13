CREATE TABLE authentication_credentials (
    access_key_id TEXT PRIMARY KEY,
    secret_access_key TEXT NOT NULL,
    principal_id TEXT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (octet_length(access_key_id) BETWEEN 1 AND 128),
    CHECK (octet_length(secret_access_key) BETWEEN 1 AND 256),
    CHECK (principal_id IS NULL OR octet_length(principal_id) <= 256),
    CHECK (version >= 0)
);
