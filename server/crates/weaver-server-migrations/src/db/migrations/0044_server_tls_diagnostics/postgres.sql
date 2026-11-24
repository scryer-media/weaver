CREATE TABLE IF NOT EXISTS server_tls_diagnostics (
    server_id                  BIGINT PRIMARY KEY NOT NULL
                               REFERENCES servers(id) ON DELETE CASCADE,
    cipher_suite               TEXT NOT NULL,
    honors_client_cipher_order BOOLEAN,
    probed_at_epoch_seconds    BIGINT NOT NULL DEFAULT 0
);
