-- The History page pages its name, state and category sorts in SQL. Each
-- index is on the exact expression the page orders by, ties going to the
-- job id in the same direction. LOWER folds ASCII letters alone.
CREATE INDEX IF NOT EXISTS idx_job_history_name_job
    ON job_history (LOWER(name) DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_job_history_state_job
    ON job_history ((CASE LOWER(status) WHEN 'complete' THEN 0 WHEN 'paused' THEN 2 ELSE 1 END) DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_job_history_category_name_job
    ON job_history (LOWER(COALESCE(category, '')) DESC, job_id DESC);
