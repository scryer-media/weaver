CREATE TABLE IF NOT EXISTS schema_version (
    version BIGINT NOT NULL
);

INSERT INTO schema_version (version)
SELECT 25
WHERE NOT EXISTS (SELECT 1 FROM schema_version);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS servers (
    id                  BIGINT PRIMARY KEY NOT NULL,
    host                TEXT NOT NULL,
    port                BIGINT NOT NULL,
    tls                 BOOLEAN NOT NULL DEFAULT TRUE,
    username            TEXT,
    password            TEXT,
    connections         BIGINT NOT NULL DEFAULT 10,
    active              BOOLEAN NOT NULL DEFAULT TRUE,
    supports_pipelining BOOLEAN NOT NULL DEFAULT FALSE,
    priority            BIGINT NOT NULL DEFAULT 0,
    tls_ca_cert         TEXT
);

CREATE TABLE IF NOT EXISTS job_history (
    job_id           BIGINT PRIMARY KEY NOT NULL,
    job_hash         BYTEA,
    name             TEXT NOT NULL,
    status           TEXT NOT NULL,
    error_message    TEXT,
    total_bytes      BIGINT NOT NULL DEFAULT 0,
    downloaded_bytes BIGINT NOT NULL DEFAULT 0,
    optional_recovery_bytes BIGINT NOT NULL DEFAULT 0,
    optional_recovery_downloaded_bytes BIGINT NOT NULL DEFAULT 0,
    failed_bytes     BIGINT NOT NULL DEFAULT 0,
    health           BIGINT NOT NULL DEFAULT 1000,
    category         TEXT,
    output_dir       TEXT,
    nzb_path         TEXT,
    nzb_zstd         BYTEA,
    created_at       BIGINT NOT NULL,
    completed_at     BIGINT NOT NULL,
    metadata         TEXT,
    last_diagnostic_id TEXT,
    last_diagnostic_uploaded_at_epoch_ms BIGINT
);

CREATE INDEX IF NOT EXISTS idx_job_history_completed_at
    ON job_history(completed_at);

CREATE TABLE IF NOT EXISTS active_jobs (
    job_id       BIGINT PRIMARY KEY NOT NULL,
    nzb_hash     BYTEA NOT NULL,
    nzb_path     TEXT NOT NULL,
    nzb_zstd     BYTEA,
    output_dir   TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'downloading',
    download_state TEXT,
    post_state   TEXT,
    run_state    TEXT,
    error        TEXT,
    created_at   BIGINT NOT NULL,
    normalization_retried BOOLEAN NOT NULL DEFAULT FALSE,
    queued_repair_at_epoch_ms DOUBLE PRECISION,
    queued_extract_at_epoch_ms DOUBLE PRECISION,
    paused_resume_status TEXT,
    paused_resume_download_state TEXT,
    paused_resume_post_state TEXT,
    category     TEXT,
    metadata     TEXT
);

CREATE TABLE IF NOT EXISTS active_segments (
    job_id          BIGINT NOT NULL,
    file_index      BIGINT NOT NULL,
    segment_number  BIGINT NOT NULL,
    file_offset     BIGINT NOT NULL,
    decoded_size    BIGINT NOT NULL,
    crc32           BIGINT NOT NULL,
    PRIMARY KEY (job_id, file_index, segment_number)
);

CREATE TABLE IF NOT EXISTS active_file_progress (
    job_id                  BIGINT NOT NULL,
    file_index              BIGINT NOT NULL,
    contiguous_bytes_written BIGINT NOT NULL,
    PRIMARY KEY (job_id, file_index)
);

CREATE TABLE IF NOT EXISTS active_files (
    job_id      BIGINT NOT NULL,
    file_index  BIGINT NOT NULL,
    filename    TEXT NOT NULL,
    md5         BYTEA NOT NULL,
    PRIMARY KEY (job_id, file_index)
);

CREATE TABLE IF NOT EXISTS active_par2 (
    job_id               BIGINT PRIMARY KEY NOT NULL,
    slice_size           BIGINT NOT NULL,
    recovery_block_count BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS active_par2_files (
    job_id               BIGINT NOT NULL,
    file_index           BIGINT NOT NULL,
    filename             TEXT NOT NULL,
    recovery_block_count BIGINT NOT NULL DEFAULT 0,
    promoted             BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (job_id, file_index)
);

CREATE TABLE IF NOT EXISTS active_extracted (
    job_id      BIGINT NOT NULL,
    member_name TEXT NOT NULL,
    output_path TEXT NOT NULL,
    PRIMARY KEY (job_id, member_name)
);

CREATE TABLE IF NOT EXISTS active_failed_extractions (
    job_id      BIGINT NOT NULL,
    member_name TEXT NOT NULL,
    PRIMARY KEY (job_id, member_name)
);

CREATE TABLE IF NOT EXISTS job_events (
    id        BIGSERIAL PRIMARY KEY,
    job_id    BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    kind      TEXT NOT NULL,
    message   TEXT NOT NULL,
    file_id   TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_id
    ON job_events(job_id);

CREATE TABLE IF NOT EXISTS integration_events (
    id           BIGSERIAL PRIMARY KEY,
    timestamp    BIGINT NOT NULL,
    kind         TEXT NOT NULL,
    item_id      BIGINT,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_integration_events_item_id
    ON integration_events(item_id);

CREATE TABLE IF NOT EXISTS active_extraction_chunks (
    job_id        BIGINT NOT NULL,
    set_name      TEXT NOT NULL,
    member_name   TEXT NOT NULL,
    volume_index  BIGINT NOT NULL,
    bytes_written BIGINT NOT NULL,
    temp_path     TEXT NOT NULL,
    start_offset  BIGINT NOT NULL DEFAULT 0,
    end_offset    BIGINT NOT NULL DEFAULT 0,
    verified      BOOLEAN NOT NULL DEFAULT FALSE,
    appended      BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (job_id, set_name, member_name, volume_index)
);

CREATE TABLE IF NOT EXISTS active_archive_headers (
    job_id    BIGINT NOT NULL,
    set_name  TEXT NOT NULL,
    headers   BYTEA NOT NULL,
    PRIMARY KEY (job_id, set_name)
);

CREATE TABLE IF NOT EXISTS active_rar_volume_facts (
    job_id      BIGINT NOT NULL,
    set_name    TEXT NOT NULL,
    volume_index BIGINT NOT NULL,
    facts_blob  BYTEA NOT NULL,
    PRIMARY KEY (job_id, set_name, volume_index)
);

CREATE TABLE IF NOT EXISTS active_detected_archives (
    job_id       BIGINT NOT NULL,
    file_index   BIGINT NOT NULL,
    kind         TEXT NOT NULL,
    set_name     TEXT NOT NULL,
    volume_index BIGINT,
    PRIMARY KEY (job_id, file_index)
);

CREATE TABLE IF NOT EXISTS active_file_identities (
    job_id                BIGINT NOT NULL,
    file_index            BIGINT NOT NULL,
    source_filename       TEXT NOT NULL,
    current_filename      TEXT NOT NULL,
    canonical_filename    TEXT,
    classification_kind   TEXT,
    classification_set_name TEXT,
    classification_volume_index BIGINT,
    classification_source TEXT NOT NULL DEFAULT 'declared',
    PRIMARY KEY (job_id, file_index)
);

CREATE TABLE IF NOT EXISTS active_volume_status (
    job_id       BIGINT NOT NULL,
    set_name     TEXT NOT NULL,
    volume_index BIGINT NOT NULL,
    extracted    BOOLEAN NOT NULL DEFAULT FALSE,
    par2_clean   BOOLEAN NOT NULL DEFAULT FALSE,
    deleted      BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (job_id, set_name, volume_index)
);

CREATE TABLE IF NOT EXISTS active_rar_verified_suspect (
    job_id       BIGINT NOT NULL,
    set_name     TEXT NOT NULL,
    volume_index BIGINT NOT NULL,
    PRIMARY KEY (job_id, set_name, volume_index)
);

CREATE TABLE IF NOT EXISTS bandwidth_usage_minute_buckets (
    bucket_epoch_minute BIGINT PRIMARY KEY NOT NULL,
    payload_bytes       BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    key_hash     BYTEA NOT NULL UNIQUE,
    scope        TEXT NOT NULL DEFAULT 'integration',
    created_at   BIGINT NOT NULL,
    last_used_at BIGINT
);

CREATE TABLE IF NOT EXISTS auth_credentials (
    id            BIGINT PRIMARY KEY CHECK (id = 1),
    username      TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    created_at    BIGINT NOT NULL,
    updated_at    BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
    id       BIGINT PRIMARY KEY NOT NULL,
    name     TEXT NOT NULL,
    dest_dir TEXT,
    aliases  TEXT NOT NULL DEFAULT ''
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_categories_name_nocase
    ON categories (LOWER(name));

CREATE TABLE IF NOT EXISTS rss_feeds (
    id                 BIGINT PRIMARY KEY NOT NULL,
    name               TEXT NOT NULL,
    url                TEXT NOT NULL,
    enabled            BOOLEAN NOT NULL DEFAULT TRUE,
    poll_interval_secs BIGINT NOT NULL DEFAULT 900,
    username           TEXT,
    password           TEXT,
    default_category   TEXT,
    default_metadata   TEXT,
    etag               TEXT,
    last_modified      TEXT,
    last_polled_at     BIGINT,
    last_success_at    BIGINT,
    last_error         TEXT,
    consecutive_failures BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rss_rules (
    id               BIGINT PRIMARY KEY NOT NULL,
    feed_id          BIGINT NOT NULL REFERENCES rss_feeds(id) ON DELETE CASCADE,
    sort_order       BIGINT NOT NULL,
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    action           TEXT NOT NULL,
    title_regex      TEXT,
    item_categories  TEXT,
    min_size_bytes   BIGINT,
    max_size_bytes   BIGINT,
    category_override TEXT,
    metadata         TEXT
);

CREATE INDEX IF NOT EXISTS idx_rss_rules_feed_sort
    ON rss_rules(feed_id, sort_order, id);

CREATE TABLE IF NOT EXISTS rss_seen_items (
    feed_id       BIGINT NOT NULL REFERENCES rss_feeds(id) ON DELETE CASCADE,
    item_id       TEXT NOT NULL,
    item_title    TEXT NOT NULL,
    published_at  BIGINT,
    size_bytes    BIGINT,
    decision      TEXT NOT NULL,
    seen_at       BIGINT NOT NULL,
    job_id        BIGINT,
    item_url      TEXT,
    error         TEXT,
    PRIMARY KEY (feed_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_rss_seen_seen_at
    ON rss_seen_items(seen_at);

CREATE TABLE IF NOT EXISTS diagnostic_runs (
    source_job_id BIGINT PRIMARY KEY NOT NULL,
    diagnostic_job_id BIGINT NOT NULL,
    smg_diagnostic_id TEXT,
    stage TEXT NOT NULL,
    include_server_hostnames BOOLEAN NOT NULL DEFAULT TRUE,
    rerun_succeeded BOOLEAN,
    error_message TEXT,
    created_at_epoch_ms BIGINT NOT NULL,
    updated_at_epoch_ms BIGINT NOT NULL,
    last_activity_at_epoch_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_diagnostic_runs_job
    ON diagnostic_runs(diagnostic_job_id);

CREATE INDEX IF NOT EXISTS idx_diagnostic_runs_activity
    ON diagnostic_runs(last_activity_at_epoch_ms, stage);

CREATE TABLE IF NOT EXISTS async_operations (
    id           BIGSERIAL PRIMARY KEY,
    kind         TEXT NOT NULL,
    state        TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    requested_at BIGINT NOT NULL,
    started_at   BIGINT,
    finished_at  BIGINT
);

CREATE INDEX IF NOT EXISTS idx_async_operations_kind_state_requested
    ON async_operations(kind, state, requested_at, id);

CREATE TABLE IF NOT EXISTS async_operation_targets (
    operation_id   BIGINT NOT NULL REFERENCES async_operations(id) ON DELETE CASCADE,
    target_kind    TEXT NOT NULL,
    target_id      BIGINT NOT NULL,
    state          TEXT NOT NULL,
    error_message  TEXT,
    sort_order     BIGINT NOT NULL,
    PRIMARY KEY (operation_id, target_kind, target_id)
);

CREATE INDEX IF NOT EXISTS idx_async_operation_targets_target_state
    ON async_operation_targets(target_kind, target_id, state);

CREATE INDEX IF NOT EXISTS idx_async_operation_targets_operation_sort
    ON async_operation_targets(operation_id, target_kind, sort_order, target_id);

CREATE TABLE IF NOT EXISTS metrics_history_chunks (
    resolution_sec        BIGINT NOT NULL,
    chunk_start_epoch_sec BIGINT NOT NULL,
    body_zstd             BYTEA NOT NULL,
    PRIMARY KEY (resolution_sec, chunk_start_epoch_sec)
);
