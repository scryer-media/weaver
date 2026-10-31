-- The engine a job runs on, so a restart keeps it whatever WEAVER_ENGINE says
-- now. Every job before this migration ran on the classic engine.
ALTER TABLE active_jobs ADD COLUMN engine TEXT NOT NULL DEFAULT 'classic';
ALTER TABLE job_history ADD COLUMN engine TEXT NOT NULL DEFAULT 'classic';
-- The phase of a graph job's root namespace: downloading, verifying,
-- repairing, extracting or done. NULL for a classic job.
ALTER TABLE active_jobs ADD COLUMN root_phase TEXT;
