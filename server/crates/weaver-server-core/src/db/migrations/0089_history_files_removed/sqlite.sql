-- When the operator removed a finished job's files and kept its history
-- entry, in epoch seconds. NULL while the files are still there to remove.
-- Nothing filters or sorts on it, so it has no index.
ALTER TABLE job_history ADD COLUMN files_removed_at INTEGER;

-- How many files the removed folder held, and their bytes, all of them.
-- NULL until a removal records them, and again once the job finishes anew.
ALTER TABLE job_history ADD COLUMN removed_file_count INTEGER;
ALTER TABLE job_history ADD COLUMN removed_file_bytes INTEGER;

-- The first files of a history entry's removed output folder, in the order
-- the job page lists them, `ordinal` counting from 0. The page reads a range
-- of the primary key. The rows go with the history entry, and are cleared
-- when the job finishes again.
CREATE TABLE job_history_removed_files (
    job_id     INTEGER NOT NULL,
    ordinal    INTEGER NOT NULL,
    rel_path   TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    PRIMARY KEY (job_id, ordinal)
) WITHOUT ROWID;

-- How a job's download went, as one versioned compact JSON document built
-- once as the job finishes: its archive tree (at most 200 nodes, with the
-- true count and depth), its recovery sets and its post. It never holds a
-- password. Only the job page reads it; the history lists never select it.
-- NULL for a job that finished before this column, and on the active row
-- until the job finishes.
ALTER TABLE active_jobs ADD COLUMN download_summary TEXT;
ALTER TABLE job_history ADD COLUMN download_summary TEXT;

-- Which jobs record an output folder: a removal that finds a folder marked
-- before markers named their job removes it only when no other job records
-- the same folder, checked once per request for every folder of it.
CREATE INDEX idx_job_history_output_dir ON job_history(output_dir, job_id);
CREATE INDEX idx_active_jobs_output_dir ON active_jobs(output_dir, job_id);

-- How a files removal ended for one entry (removed, already_gone, not_found,
-- not_managed, busy, failed, or superseded by a delete), shown with the
-- target state; NULL for a delete, whose state and message say all.
ALTER TABLE async_operation_targets ADD COLUMN outcome TEXT;
