-- Each posted file of a job's NZB, as a job waiting its turn holds it: the
-- name it was posted under, its groups, its date, and how many articles it
-- has and their size. Nothing per article: the article lists stay in the
-- stored NZB. A restart builds every waiting job from these rows. They go
-- with the job's other active rows.
CREATE TABLE active_job_nzb_files (
    job_id     BIGINT NOT NULL,
    file_index BIGINT NOT NULL,
    full_name  TEXT NOT NULL,
    groups     TEXT NOT NULL,
    posted_at  BIGINT NOT NULL,
    articles   BIGINT NOT NULL,
    bytes      BIGINT NOT NULL,
    PRIMARY KEY (job_id, file_index)
);

-- The NZB's title and first password beside them, and how many files the
-- outline holds. NULL files: the job has no outline yet, and the first read
-- of its stored NZB writes one.
ALTER TABLE active_jobs ADD COLUMN nzb_title TEXT;
ALTER TABLE active_jobs ADD COLUMN nzb_password TEXT;
ALTER TABLE active_jobs ADD COLUMN nzb_files BIGINT;
