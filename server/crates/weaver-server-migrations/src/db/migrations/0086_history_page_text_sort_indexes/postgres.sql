-- The History page pages its name, state and category sorts in SQL. Each
-- index is on the exact expression the page orders by, ties going to the
-- job id in the same direction. translate folds ASCII letters alone, as
-- SQLite's LOWER does, and the C collation compares bytes, as SQLite does.
CREATE INDEX IF NOT EXISTS idx_job_history_name_job
    ON job_history ((translate(name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz') COLLATE "C") DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_job_history_state_job
    ON job_history ((CASE translate(status, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz') WHEN 'complete' THEN 0 WHEN 'paused' THEN 2 ELSE 1 END) DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_job_history_category_name_job
    ON job_history ((translate(COALESCE(category, ''), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz') COLLATE "C") DESC, job_id DESC);
