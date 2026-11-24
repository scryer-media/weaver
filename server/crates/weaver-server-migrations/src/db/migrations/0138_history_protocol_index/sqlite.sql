-- The History page keeps one class of job (usenet or torrent) and counts the
-- rows of each. The filter, newest first, and the count are served by one
-- index on the class.
CREATE INDEX IF NOT EXISTS idx_job_history_protocol_completed_job
    ON job_history (protocol, completed_at DESC, job_id DESC);
