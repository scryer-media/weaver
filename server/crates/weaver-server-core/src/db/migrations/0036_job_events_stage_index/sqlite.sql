CREATE INDEX IF NOT EXISTS idx_job_events_job_kind_ts ON job_events(job_id, kind, timestamp);
