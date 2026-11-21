-- The name sort orders by the full release name, ties going to the job id in
-- the same direction. LOWER folds ASCII letters alone. The index on the
-- shortened name served the sort before it and serves nothing now.
CREATE INDEX IF NOT EXISTS idx_job_history_release_name_job
    ON job_history (LOWER(release_name) DESC, job_id DESC);
DROP INDEX IF EXISTS idx_job_history_name_job;
