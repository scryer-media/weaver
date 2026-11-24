CREATE TABLE server_tls_diagnostics (
    server_id                  INTEGER PRIMARY KEY NOT NULL
                               REFERENCES servers(id) ON DELETE CASCADE,
    cipher_suite               TEXT NOT NULL,
    honors_client_cipher_order INTEGER,
    probed_at_epoch_seconds    INTEGER NOT NULL DEFAULT 0
);
