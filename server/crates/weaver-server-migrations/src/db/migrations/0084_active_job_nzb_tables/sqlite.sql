-- A job's stored NZB, compressed, one row per job, apart from the job's own
-- row. On the active row and on the history row it sat in front of every
-- column added since, so a read of any of those walked the whole NZB. A job
-- keeps its row from its admission through its history, so finishing a job
-- moves no NZB; the row goes once the job is in neither the queue nor the
-- history.
CREATE TABLE job_nzbs (
    job_id   INTEGER PRIMARY KEY,
    nzb_zstd BLOB NOT NULL
);

-- A job's NZB outline, one row per job: every posted file of it, as a job
-- waiting its turn holds it, in one compressed blob. The title, first
-- password and file count stay on the active row. It goes with the job's
-- other active rows.
CREATE TABLE active_job_outlines (
    job_id  INTEGER PRIMARY KEY,
    outline BLOB NOT NULL
);

-- Migration 85 then moves each active and finished job's stored NZB here,
-- compresses each job's outline rows into one, and drops
-- `active_jobs.nzb_zstd`, `job_history.nzb_zstd` and `active_job_nzb_files`.
