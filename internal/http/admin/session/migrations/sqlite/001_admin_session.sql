CREATE TABLE IF NOT EXISTS admin_session (
    id_hash TEXT PRIMARY KEY,
    access_key_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    credential_hash TEXT NOT NULL,
    csrf_token TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    last_seen_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    client_ip TEXT NOT NULL,
    user_agent TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS admin_session_expires_at ON admin_session(expires_at);
CREATE INDEX IF NOT EXISTS admin_session_last_seen_at ON admin_session(last_seen_at);
