-- See the SQLite step for why nothing is converted.

DROP INDEX IF EXISTS idx_active_jobs_post_processing_run;
DROP INDEX IF EXISTS idx_job_history_post_processing_run;
DROP INDEX IF EXISTS idx_post_processing_attempts_status_queued;
DROP INDEX IF EXISTS idx_post_processing_attempts_run_step;
DROP INDEX IF EXISTS idx_post_processing_runs_status_queued;
DROP INDEX IF EXISTS idx_post_processing_runs_job_queued;
DROP INDEX IF EXISTS idx_post_processing_revisions_extension_trust;

DROP TABLE IF EXISTS post_processing_log_chunks;
DROP TABLE IF EXISTS post_processing_attempts;
DROP TABLE IF EXISTS post_processing_runs;
DROP TABLE IF EXISTS post_processing_job_plans;
DROP TABLE IF EXISTS post_processing_profile_assignments;
DROP TABLE IF EXISTS post_processing_profile_steps;
DROP TABLE IF EXISTS post_processing_profiles;
DROP TABLE IF EXISTS post_processing_extension_revisions;

ALTER TABLE active_jobs
    DROP COLUMN IF EXISTS post_processing_run_id,
    DROP COLUMN IF EXISTS pipeline_outcome_json;
ALTER TABLE active_jobs ADD COLUMN IF NOT EXISTS script_results_json TEXT;

ALTER TABLE job_history
    DROP COLUMN IF EXISTS post_processing_run_id,
    DROP COLUMN IF EXISTS pipeline_outcome_json;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS script_results_json TEXT;

DELETE FROM settings WHERE key = 'post_processing.settings.v1';
