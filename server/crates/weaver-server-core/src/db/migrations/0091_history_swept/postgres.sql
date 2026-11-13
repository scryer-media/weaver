-- When housekeeping swept a failed job's files and left nothing of it on
-- disk, in epoch seconds. Internal: nothing the history page, the job page
-- or the API shows reads it. NULL until a sweep leaves nothing of the job,
-- and again once the job finishes anew.
ALTER TABLE job_history ADD COLUMN files_swept_at BIGINT;

-- The sweep's page: failed jobs not yet swept, newest first, at or before a
-- cutoff. The predicate is literal in the sweep's query, so the planner
-- serves the filter and the sort from this index, read backward, and an
-- idle sweep reads an empty range. The leading status column makes it the
-- planner's choice over the plain status index.
CREATE INDEX idx_job_history_failed_unswept
    ON job_history(status, completed_at, job_id)
    WHERE status = 'failed' AND files_swept_at IS NULL;
