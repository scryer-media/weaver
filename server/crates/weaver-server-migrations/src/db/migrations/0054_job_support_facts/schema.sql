-- A job's direct-store demotion and its article-gap summary, as one compact
-- JSON document written by the per-job support facts. Fixed in size whatever
-- the job does, NULL for a job with nothing to report, and reporting only:
-- nothing reads it to make a scheduling, repair or extraction decision.
ALTER TABLE active_jobs ADD COLUMN support_facts TEXT;
ALTER TABLE job_history ADD COLUMN support_facts TEXT;
