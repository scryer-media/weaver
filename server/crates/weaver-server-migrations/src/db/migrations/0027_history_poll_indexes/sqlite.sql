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

CREATE INDEX IF NOT EXISTS idx_job_history_completed_job
    ON job_history (completed_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS idx_job_history_status_completed_job
    ON job_history (status, completed_at DESC, job_id DESC);
