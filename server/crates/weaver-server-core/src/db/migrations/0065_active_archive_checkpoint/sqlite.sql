CREATE TABLE active_archive_checkpoint (
    job_id INTEGER NOT NULL PRIMARY KEY,
    generation INTEGER NOT NULL CHECK (generation > 0),
    reference BLOB NOT NULL CHECK (length(reference) BETWEEN 1 AND 256)
) WITHOUT ROWID;
