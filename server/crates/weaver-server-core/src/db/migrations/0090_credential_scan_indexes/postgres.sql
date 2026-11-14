-- Start-up checks every stored credential against the database key. Each
-- read below scanned the whole of active_jobs or job_history, every row of
-- the queue and of history with its metadata, to find the few rows that hold
-- a credential. Each index holds only those rows, so a read walks them alone.
-- An index is not a view: nothing reads a password through one that could
-- not read the column already, and each password stays sealed as stored.

-- The jobs whose metadata holds the sealed password an archive opened with.
-- The predicate is the read's own, word for word, so the read is served by
-- the index.
CREATE INDEX IF NOT EXISTS idx_active_jobs_validated_archive_password
    ON active_jobs(job_id) WHERE metadata LIKE '%validated_archive_password%';
CREATE INDEX IF NOT EXISTS idx_job_history_validated_archive_password
    ON job_history(job_id) WHERE metadata LIKE '%validated_archive_password%';

-- The jobs with a password; the reads take the job and the sealed password
-- from the index alone.
CREATE INDEX IF NOT EXISTS idx_active_jobs_password
    ON active_jobs(job_id) INCLUDE (password) WHERE password IS NOT NULL;

-- The jobs with password candidates, which can be long, so only the job is
-- in the index.
CREATE INDEX IF NOT EXISTS idx_active_jobs_password_candidates
    ON active_jobs(job_id) WHERE password_candidates IS NOT NULL;
