DROP TABLE IF EXISTS job_history_attributes;

CREATE TABLE job_history_new (
    job_id           INTEGER PRIMARY KEY NOT NULL,
    job_hash         BLOB,
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
    nzb_zstd         BLOB,
    created_at       INTEGER NOT NULL,
    completed_at     INTEGER NOT NULL,
    metadata         TEXT
);

INSERT INTO job_history_new (
    job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
    optional_recovery_bytes, optional_recovery_downloaded_bytes, failed_bytes,
    health, category, output_dir, nzb_path, nzb_zstd, created_at, completed_at, metadata
)
SELECT
    job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
    optional_recovery_bytes, optional_recovery_downloaded_bytes, failed_bytes,
    health, category, output_dir, nzb_path, nzb_zstd, created_at, completed_at,
    CASE
        WHEN metadata IS NULL OR NOT json_valid(metadata) THEN metadata
        ELSE (
            SELECT COALESCE(json_group_array(json(attr.value)), '[]')
            FROM json_each(metadata) AS attr
            WHERE COALESCE(json_extract(attr.value, '$[0]'), '') NOT IN (
                '__weaver_diagnostic_source_job_id',
                '__weaver_diagnostic_include_server_hostnames'
            )
        )
    END AS metadata
FROM job_history;

DROP TABLE job_history;

ALTER TABLE job_history_new RENAME TO job_history;

CREATE INDEX IF NOT EXISTS idx_job_history_completed_at
    ON job_history(completed_at);

CREATE INDEX IF NOT EXISTS idx_job_history_completed_job
    ON job_history (completed_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS idx_job_history_status_completed_job
    ON job_history (status, completed_at DESC, job_id DESC);

CREATE TABLE IF NOT EXISTS job_history_attributes (
    job_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    completed_at INTEGER NOT NULL,
    PRIMARY KEY (job_id, key, value),
    FOREIGN KEY (job_id) REFERENCES job_history(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_history_attributes_key_value_completed_job
    ON job_history_attributes (key, value, completed_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS idx_job_history_attributes_key_completed_job
    ON job_history_attributes (key, completed_at DESC, job_id DESC);

INSERT OR IGNORE INTO job_history_attributes (job_id, key, value, completed_at)
SELECT
    h.job_id,
    json_extract(attr.value, '$[0]') AS key,
    json_extract(attr.value, '$[1]') AS value,
    h.completed_at
FROM job_history h,
     json_each(CASE WHEN json_valid(h.metadata) THEN h.metadata ELSE '[]' END) AS attr
WHERE h.metadata IS NOT NULL
  AND json_valid(h.metadata)
  AND json_type(attr.value) = 'array'
  AND json_array_length(attr.value) = 2
  AND json_type(attr.value, '$[0]') = 'text'
  AND json_type(attr.value, '$[1]') = 'text'
  AND json_extract(attr.value, '$[0]') NOT IN (
      '__weaver_client_request_id',
      '__weaver_diagnostic_source_job_id',
      '__weaver_diagnostic_include_server_hostnames'
  );

DROP TABLE IF EXISTS diagnostic_runs;

DELETE FROM settings WHERE key = 'diagnostic_upload_url';
