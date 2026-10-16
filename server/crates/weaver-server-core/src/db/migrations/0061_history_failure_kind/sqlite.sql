-- Why a failed job failed: repair, password, disk_full, unpack, move, health
-- or duplicate. NULL for a job that did not fail, or that failed before
-- weaver kept the kind.
ALTER TABLE job_history ADD COLUMN failure_kind TEXT;
