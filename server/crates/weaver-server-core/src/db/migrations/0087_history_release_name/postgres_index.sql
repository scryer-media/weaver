-- The name sort orders by the full release name, ties going to the job id in
-- the same direction. translate folds ASCII letters alone, as SQLite's LOWER
-- does, and the C collation compares bytes, as SQLite does. The index on the
-- shortened name served the sort before it and serves nothing now.
CREATE INDEX IF NOT EXISTS idx_job_history_release_name_job
    ON job_history ((translate(release_name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz') COLLATE "C") DESC, job_id DESC);
DROP INDEX IF EXISTS idx_job_history_name_job;
