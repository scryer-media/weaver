CREATE TABLE IF NOT EXISTS job_history_attributes (
    job_id BIGINT NOT NULL REFERENCES job_history(job_id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    completed_at BIGINT NOT NULL,
    PRIMARY KEY (job_id, key, value)
);

CREATE INDEX IF NOT EXISTS idx_job_history_attributes_key_value_completed_job
    ON job_history_attributes (key, value, completed_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS idx_job_history_attributes_key_completed_job
    ON job_history_attributes (key, completed_at DESC, job_id DESC);

INSERT INTO job_history_attributes (job_id, key, value, completed_at)
SELECT
    h.job_id,
    attr.value ->> 0 AS key,
    attr.value ->> 1 AS value,
    h.completed_at
FROM job_history h
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(h.metadata::jsonb, '[]'::jsonb)) AS attr(value)
WHERE h.metadata IS NOT NULL
  AND jsonb_typeof(attr.value) = 'array'
  AND jsonb_array_length(attr.value) = 2
  AND jsonb_typeof(attr.value -> 0) = 'string'
  AND jsonb_typeof(attr.value -> 1) = 'string'
  AND attr.value ->> 0 NOT IN (
      '__weaver_client_request_id',
      '__weaver_diagnostic_source_job_id',
      '__weaver_diagnostic_include_server_hostnames'
  )
ON CONFLICT (job_id, key, value) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_job_history_completed_job
    ON job_history (completed_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS idx_job_history_status_completed_job
    ON job_history (status, completed_at DESC, job_id DESC);
