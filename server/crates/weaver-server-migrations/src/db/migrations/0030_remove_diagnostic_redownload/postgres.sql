DROP TABLE IF EXISTS diagnostic_runs;

ALTER TABLE job_history
    DROP COLUMN IF EXISTS last_diagnostic_id,
    DROP COLUMN IF EXISTS last_diagnostic_uploaded_at_epoch_ms;

-- Tolerant text->jsonb parse. `h.metadata::jsonb` raises on unparseable input,
-- so a single externally-corrupted job_history.metadata row would abort the
-- whole migration transaction. Mirror the SQLite payload, which leaves rows
-- with NULL/invalid metadata untouched and only rewrites valid JSON arrays.
-- pg16-only pg_input_is_valid() is intentionally avoided for older-server
-- compatibility; the function lives in pg_temp and is dropped afterward.
CREATE OR REPLACE FUNCTION pg_temp.weaver_mig0030_try_jsonb(input TEXT)
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

UPDATE job_history AS h
   SET metadata = COALESCE(
       (
           SELECT jsonb_agg(attr.value ORDER BY attr.ordinality)::text
             FROM jsonb_array_elements(pg_temp.weaver_mig0030_try_jsonb(h.metadata))
                  WITH ORDINALITY AS attr(value, ordinality)
            WHERE COALESCE(attr.value #>> '{0}', '') NOT IN (
                '__weaver_diagnostic_source_job_id',
                '__weaver_diagnostic_include_server_hostnames'
            )
       ),
       '[]'
   )
 WHERE h.metadata IS NOT NULL
   AND jsonb_typeof(pg_temp.weaver_mig0030_try_jsonb(h.metadata)) = 'array';

DROP FUNCTION IF EXISTS pg_temp.weaver_mig0030_try_jsonb(TEXT);

DELETE FROM job_history_attributes
 WHERE key IN (
    '__weaver_diagnostic_source_job_id',
    '__weaver_diagnostic_include_server_hostnames'
 );

DELETE FROM settings WHERE key = 'diagnostic_upload_url';
