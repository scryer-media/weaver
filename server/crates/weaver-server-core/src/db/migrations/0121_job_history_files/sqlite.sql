-- The files a torrent's job wrote, the torrent's own and what unpacking
-- placed beside them, as absolute paths, listed as its history row is
-- written: an archive from history removes exactly these, a `whole` one
-- (what unpacking placed) with what is in it, any other only as a file. An
-- NZB job lists none; its archive goes by its marked folders. The rows go
-- with the history row, in the transaction that deletes it, and are written
-- anew, in place of the last list, each time the job's row is. The primary
-- key serves every read and delete, which go by job.
CREATE TABLE job_history_files (
    job_id INTEGER NOT NULL,
    path   TEXT NOT NULL,
    whole  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, path)
);

-- What each live torrent holds where its files are: the first part of each
-- of its files' paths and of what unpacking placed beside them, the folder
-- claimed for it when its files lie elsewhere, or its whole folder while
-- its files are not known, each as an absolute path spelled as the
-- filesystem spells it, so two spellings of one folder (a link, another
-- case) are one. An archive or delete from history leaves what these hold
-- under the folder it removes from, found by the path index: the paths
-- equal to or under that folder as a range, and those above it.
-- `held_set` says the torrent's rows are its own; a torrent kept before
-- them holds its whole folder, as spelled, until its next start writes
-- them. The rows go with the torrent.
ALTER TABLE torrents ADD COLUMN held_set INTEGER NOT NULL DEFAULT 0;

CREATE TABLE torrent_held_paths (
    job_id INTEGER NOT NULL,
    path   TEXT NOT NULL,
    PRIMARY KEY (job_id, path)
);
CREATE INDEX idx_torrent_held_paths_path ON torrent_held_paths (path);

INSERT INTO torrent_held_paths (job_id, path)
SELECT job_id, COALESCE(files_dir, storage_dir) FROM torrents
 WHERE COALESCE(files_dir, storage_dir) IS NOT NULL;
