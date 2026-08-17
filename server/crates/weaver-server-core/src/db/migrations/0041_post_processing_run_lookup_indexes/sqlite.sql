-- Finishing a post-processing run stamps its summary onto the job's rows:
--
--   UPDATE active_jobs SET post_processing_summary = ? WHERE post_processing_run_id = ?
--   UPDATE job_history SET post_processing_summary = ? WHERE post_processing_run_id = ?
--
-- Migration 0037 added `post_processing_run_id` to both tables without an index,
-- so both statements are full table scans. `active_jobs` is bounded by the live
-- queue, but `job_history` grows for the life of the install, which makes every
-- run finalize slower the longer weaver has been running — on an idle machine,
-- with no contention involved.
--
-- Both columns are NULL for every row that never ran an extension, so these are
-- partial indexes: they stay small on installs that do not use post-processing
-- while still serving the equality lookup on the ones that do.
CREATE INDEX IF NOT EXISTS idx_active_jobs_post_processing_run
    ON active_jobs(post_processing_run_id)
    WHERE post_processing_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_job_history_post_processing_run
    ON job_history(post_processing_run_id)
    WHERE post_processing_run_id IS NOT NULL;
