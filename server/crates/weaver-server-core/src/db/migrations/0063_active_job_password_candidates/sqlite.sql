-- Indexer-supplied archive passwords for an active job, sealed as one value
-- the way the job's password is. NULL when the job has none.
ALTER TABLE active_jobs ADD COLUMN password_candidates TEXT;
