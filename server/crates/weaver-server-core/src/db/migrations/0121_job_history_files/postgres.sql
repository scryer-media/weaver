-- The files a torrent's job wrote, the torrent's own and what unpacking
-- placed beside them, as absolute paths, listed as its history row is
-- written: an archive from history removes exactly these, a `whole` one
-- (what unpacking placed) with what is in it, any other only as a file. An
-- NZB job lists none; its archive goes by its marked folders. The rows go
-- with the history row, in the transaction that deletes it, and are written
-- anew, in place of the last list, each time the job's row is. The primary
-- key serves every read and delete, which go by job.
CREATE TABLE job_history_files (
    job_id BIGINT NOT NULL,
    path   TEXT NOT NULL,
    whole  BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (job_id, path)
);
