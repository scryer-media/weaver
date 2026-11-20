-- Each file's modified time, in nanoseconds since the epoch, as a torrent's
-- checkpoint took it with the pieces it kept: the next start checks again
-- only the pieces of a file whose time is no longer this one. A file with no
-- usable time has no row, and is judged by its size alone. One row per file
-- of a torrent, keyed by both; it goes with the torrent. A new table, empty
-- until a checkpoint writes it: nothing to backfill.
CREATE TABLE torrent_file_stamps (
    job_id      INTEGER NOT NULL REFERENCES torrents(job_id) ON DELETE CASCADE,
    file        INTEGER NOT NULL,
    modified_ns INTEGER NOT NULL,
    PRIMARY KEY (job_id, file)
) WITHOUT ROWID;
