-- The History page pages in SQL when it sorts by size or integrity, or keeps
-- only some categories. Each order is served by its own index, ties going to
-- the job id in the same direction, and the category filter matches the
-- expression a row with no category is counted under.
CREATE INDEX IF NOT EXISTS idx_job_history_size_job
    ON job_history (total_bytes DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_job_history_health_job
    ON job_history (health DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_job_history_category_completed_job
    ON job_history ((COALESCE(category, '')), completed_at DESC, job_id DESC);
