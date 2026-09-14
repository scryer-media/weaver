CREATE TABLE IF NOT EXISTS browser_sessions (
    token_hash TEXT PRIMARY KEY NOT NULL,
    csrf_verifier TEXT NOT NULL,
    origin TEXT NOT NULL,
    client_ip TEXT,
    remembered BOOLEAN NOT NULL DEFAULT FALSE,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    revoked_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_browser_sessions_expiry
    ON browser_sessions(expires_at);

CREATE TABLE IF NOT EXISTS browser_session_verifications (
    token_hash TEXT PRIMARY KEY REFERENCES browser_sessions(token_hash),
    verified_at BIGINT NOT NULL
);
