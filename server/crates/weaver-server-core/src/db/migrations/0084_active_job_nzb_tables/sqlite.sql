-- A job's stored NZB, compressed, one row per job, apart from its active row.
-- On the active row it sat in front of every column added since, so a read of
-- any of those walked the whole NZB. It goes with the job's other active rows.
CREATE TABLE active_job_nzbs (
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

-- The upgrade step then moves each stored NZB here, compresses each job's
-- outline rows into one, and drops `active_jobs.nzb_zstd` and
-- `active_job_nzb_files`.
