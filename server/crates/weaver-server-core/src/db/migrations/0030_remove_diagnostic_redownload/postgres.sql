DROP TABLE IF EXISTS diagnostic_runs;

ALTER TABLE job_history
    DROP COLUMN IF EXISTS last_diagnostic_id,
    DROP COLUMN IF EXISTS last_diagnostic_uploaded_at_epoch_ms;

UPDATE job_history AS h
   SET metadata = COALESCE(
       (
           SELECT jsonb_agg(attr.value ORDER BY attr.ordinality)::text
             FROM jsonb_array_elements(h.metadata::jsonb) WITH ORDINALITY AS attr(value, ordinality)
            WHERE COALESCE(attr.value #>> '{0}', '') NOT IN (
                '__weaver_diagnostic_source_job_id',
                '__weaver_diagnostic_include_server_hostnames'
            )
       ),
       '[]'
   )
 WHERE h.metadata IS NOT NULL
   AND jsonb_typeof(h.metadata::jsonb) = 'array';

DELETE FROM job_history_attributes
 WHERE key IN (
    '__weaver_diagnostic_source_job_id',
    '__weaver_diagnostic_include_server_hostnames'
 );

DELETE FROM settings WHERE key = 'diagnostic_upload_url';
