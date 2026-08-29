-- Post-processing becomes "run some scripts after the job finishes".
--
-- A script is a file in data_dir/scripts with no database identity, so the
-- revision/profile/plan/run/attempt/log model has nothing left to describe.
-- Ordered script lists and per-script options move to the settings KV, and the
-- only per-job state is the summary (already present) plus the script results.
--
-- No data is converted: profiles and approvals have no successor concept, and
-- historical run detail beyond the summary already on the job is dropped.

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

-- Both columns were written only by the deleted subsystem: pipeline_outcome_json
-- was a copy of the job's own terminal state and was never read back for display.
ALTER TABLE active_jobs DROP COLUMN post_processing_run_id;
ALTER TABLE active_jobs DROP COLUMN pipeline_outcome_json;
ALTER TABLE active_jobs ADD COLUMN script_results_json TEXT;

ALTER TABLE job_history DROP COLUMN post_processing_run_id;
ALTER TABLE job_history DROP COLUMN pipeline_outcome_json;
ALTER TABLE job_history ADD COLUMN script_results_json TEXT;

-- The old settings blob carried discovery/webhook/allowed-root fields and an
-- execution flag that meant "run approved plans". The new flag means "run every
-- enabled script in the directory", so it must be granted again knowingly.
DELETE FROM settings WHERE key = 'post_processing.settings.v1';
