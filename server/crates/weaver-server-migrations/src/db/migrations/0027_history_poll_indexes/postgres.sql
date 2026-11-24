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

-- Tolerant text->jsonb parse. `text::jsonb` raises on unparseable input, and
-- jsonb_array_elements raises on a valid-but-non-array value; either aborts the
-- whole migration transaction on a single corrupt job_history.metadata row.
-- Weaver only ever writes JSON arrays or NULL, but externally-mutated rows can
-- exist. This mirrors the SQLite payload's json_valid() tolerance (bad rows are
-- silently skipped) without relying on pg16-only pg_input_is_valid(). The
-- function lives in pg_temp so it never pollutes the application schema, and it
-- is dropped explicitly once the backfill completes.
CREATE OR REPLACE FUNCTION pg_temp.weaver_mig0027_try_jsonb(input TEXT)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN input::jsonb;
EXCEPTION
    WHEN others THEN
        RETURN NULL;
END;
$$;

INSERT INTO job_history_attributes (job_id, key, value, completed_at)
SELECT
    h.job_id,
    attr.value ->> 0 AS key,
    attr.value ->> 1 AS value,
    h.completed_at
FROM job_history h
-- Feed jsonb_array_elements a value that is guaranteed to be a JSON array or
-- NULL: jsonb_array_elements(NULL) yields zero rows, so unparseable text and
-- valid-but-non-array metadata are silently skipped instead of raising.
CROSS JOIN LATERAL jsonb_array_elements(
    CASE
        WHEN jsonb_typeof(pg_temp.weaver_mig0027_try_jsonb(h.metadata)) = 'array'
            THEN pg_temp.weaver_mig0027_try_jsonb(h.metadata)
        ELSE NULL
    END
) AS attr(value)
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

DROP FUNCTION IF EXISTS pg_temp.weaver_mig0027_try_jsonb(TEXT);

CREATE INDEX IF NOT EXISTS idx_job_history_completed_job
    ON job_history (completed_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS idx_job_history_status_completed_job
    ON job_history (status, completed_at DESC, job_id DESC);
