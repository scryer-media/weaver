CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS servers (
    id                  INTEGER PRIMARY KEY NOT NULL,
    host                TEXT NOT NULL,
    port                INTEGER NOT NULL,
    tls                 INTEGER NOT NULL DEFAULT 1,
    username            TEXT,
    password            TEXT,
    connections         INTEGER NOT NULL DEFAULT 10,
    active              INTEGER NOT NULL DEFAULT 1,
    supports_pipelining INTEGER NOT NULL DEFAULT 0,
    priority            INTEGER NOT NULL DEFAULT 0,
    tls_ca_cert         TEXT
);

CREATE TABLE IF NOT EXISTS job_history (
    job_id           INTEGER PRIMARY KEY NOT NULL,
    name             TEXT NOT NULL,
    status           TEXT NOT NULL,
    error_message    TEXT,
    total_bytes      INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    optional_recovery_bytes INTEGER NOT NULL DEFAULT 0,
    optional_recovery_downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    failed_bytes     INTEGER NOT NULL DEFAULT 0,
    health           INTEGER NOT NULL DEFAULT 1000,
    category         TEXT,
    output_dir       TEXT,
    nzb_path         TEXT,
    created_at       INTEGER NOT NULL,
    completed_at     INTEGER NOT NULL,
    metadata         TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_history_completed_at
    ON job_history(completed_at);

CREATE TABLE IF NOT EXISTS active_jobs (
    job_id       INTEGER PRIMARY KEY NOT NULL,
    nzb_hash     BLOB NOT NULL,
    nzb_path     TEXT NOT NULL,
    output_dir   TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'downloading',
    download_state TEXT,
    post_state   TEXT,
    run_state    TEXT,
    error        TEXT,
    created_at   INTEGER NOT NULL,
    normalization_retried INTEGER NOT NULL DEFAULT 0,
    queued_repair_at_epoch_ms REAL,
    queued_extract_at_epoch_ms REAL,
    paused_resume_status TEXT,
    paused_resume_download_state TEXT,
    paused_resume_post_state TEXT,
    category     TEXT,
    metadata     TEXT
);

CREATE TABLE IF NOT EXISTS active_segments (
    job_id          INTEGER NOT NULL,
    file_index      INTEGER NOT NULL,
    segment_number  INTEGER NOT NULL,
    file_offset     INTEGER NOT NULL,
    decoded_size    INTEGER NOT NULL,
    crc32           INTEGER NOT NULL,
    PRIMARY KEY (job_id, file_index, segment_number)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_file_progress (
    job_id                  INTEGER NOT NULL,
    file_index              INTEGER NOT NULL,
    contiguous_bytes_written INTEGER NOT NULL,
    PRIMARY KEY (job_id, file_index)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_files (
    job_id      INTEGER NOT NULL,
    file_index  INTEGER NOT NULL,
    filename    TEXT NOT NULL,
    md5         BLOB NOT NULL,
    PRIMARY KEY (job_id, file_index)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_par2 (
    job_id               INTEGER PRIMARY KEY NOT NULL,
    slice_size           INTEGER NOT NULL,
    recovery_block_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS active_par2_files (
    job_id               INTEGER NOT NULL,
    file_index           INTEGER NOT NULL,
    filename             TEXT NOT NULL,
    recovery_block_count INTEGER NOT NULL DEFAULT 0,
    promoted             INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, file_index)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_extracted (
    job_id      INTEGER NOT NULL,
    member_name TEXT NOT NULL,
    output_path TEXT NOT NULL,
    PRIMARY KEY (job_id, member_name)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_failed_extractions (
    job_id      INTEGER NOT NULL,
    member_name TEXT NOT NULL,
    PRIMARY KEY (job_id, member_name)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS job_events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id    INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    kind      TEXT NOT NULL,
    message   TEXT NOT NULL,
    file_id   TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_id
    ON job_events(job_id);

CREATE TABLE IF NOT EXISTS integration_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp    INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    item_id      INTEGER,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_integration_events_item_id
    ON integration_events(item_id);

CREATE TABLE IF NOT EXISTS metrics_scrapes (
    scraped_at_epoch_sec INTEGER PRIMARY KEY NOT NULL,
    body_zstd            BLOB NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_extraction_chunks (
    job_id        INTEGER NOT NULL,
    set_name      TEXT NOT NULL,
    member_name   TEXT NOT NULL,
    volume_index  INTEGER NOT NULL,
    bytes_written INTEGER NOT NULL,
    temp_path     TEXT NOT NULL,
    start_offset  INTEGER NOT NULL DEFAULT 0,
    end_offset    INTEGER NOT NULL DEFAULT 0,
    verified      INTEGER NOT NULL DEFAULT 0,
    appended      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, set_name, member_name, volume_index)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_archive_headers (
    job_id    INTEGER NOT NULL,
    set_name  TEXT NOT NULL,
    headers   BLOB NOT NULL,
    PRIMARY KEY (job_id, set_name)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_rar_volume_facts (
    job_id      INTEGER NOT NULL,
    set_name    TEXT NOT NULL,
    volume_index INTEGER NOT NULL,
    facts_blob  BLOB NOT NULL,
    PRIMARY KEY (job_id, set_name, volume_index)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_detected_archives (
    job_id       INTEGER NOT NULL,
    file_index   INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    set_name     TEXT NOT NULL,
    volume_index INTEGER,
    PRIMARY KEY (job_id, file_index)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_file_identities (
    job_id                INTEGER NOT NULL,
    file_index            INTEGER NOT NULL,
    source_filename       TEXT NOT NULL,
    current_filename      TEXT NOT NULL,
    canonical_filename    TEXT,
    classification_kind   TEXT,
    classification_set_name TEXT,
    classification_volume_index INTEGER,
    classification_source TEXT NOT NULL DEFAULT 'declared',
    PRIMARY KEY (job_id, file_index)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_volume_status (
    job_id       INTEGER NOT NULL,
    set_name     TEXT NOT NULL,
    volume_index INTEGER NOT NULL,
    extracted    INTEGER NOT NULL DEFAULT 0,
    par2_clean   INTEGER NOT NULL DEFAULT 0,
    deleted      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, set_name, volume_index)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS active_rar_verified_suspect (
    job_id       INTEGER NOT NULL,
    set_name     TEXT NOT NULL,
    volume_index INTEGER NOT NULL,
    PRIMARY KEY (job_id, set_name, volume_index)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS bandwidth_usage_minute_buckets (
    bucket_epoch_minute INTEGER PRIMARY KEY NOT NULL,
    payload_bytes       INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS api_keys (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    key_hash     BLOB NOT NULL UNIQUE,
    scope        TEXT NOT NULL DEFAULT 'integration',
    created_at   INTEGER NOT NULL,
    last_used_at INTEGER
);

CREATE TABLE IF NOT EXISTS auth_credentials (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    username      TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
    id       INTEGER PRIMARY KEY NOT NULL,
    name     TEXT NOT NULL UNIQUE COLLATE NOCASE,
    dest_dir TEXT,
    aliases  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS rss_feeds (
    id                 INTEGER PRIMARY KEY NOT NULL,
    name               TEXT NOT NULL,
    url                TEXT NOT NULL,
    enabled            INTEGER NOT NULL DEFAULT 1,
    poll_interval_secs INTEGER NOT NULL DEFAULT 900,
    username           TEXT,
    password           TEXT,
    default_category   TEXT,
    default_metadata   TEXT,
    etag               TEXT,
    last_modified      TEXT,
    last_polled_at     INTEGER,
    last_success_at    INTEGER,
    last_error         TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rss_rules (
    id               INTEGER PRIMARY KEY NOT NULL,
    feed_id          INTEGER NOT NULL REFERENCES rss_feeds(id) ON DELETE CASCADE,
    sort_order       INTEGER NOT NULL,
    enabled          INTEGER NOT NULL DEFAULT 1,
    action           TEXT NOT NULL,
    title_regex      TEXT,
    item_categories  TEXT,
    min_size_bytes   INTEGER,
    max_size_bytes   INTEGER,
    category_override TEXT,
    metadata         TEXT
);

CREATE INDEX IF NOT EXISTS idx_rss_rules_feed_sort
    ON rss_rules(feed_id, sort_order, id);

CREATE TABLE IF NOT EXISTS rss_seen_items (
    feed_id       INTEGER NOT NULL REFERENCES rss_feeds(id) ON DELETE CASCADE,
    item_id       TEXT NOT NULL,
    item_title    TEXT NOT NULL,
    published_at  INTEGER,
    size_bytes    INTEGER,
    decision      TEXT NOT NULL,
    seen_at       INTEGER NOT NULL,
    job_id        INTEGER,
    item_url      TEXT,
    error         TEXT,
    PRIMARY KEY (feed_id, item_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_rss_seen_seen_at
    ON rss_seen_items(seen_at);
