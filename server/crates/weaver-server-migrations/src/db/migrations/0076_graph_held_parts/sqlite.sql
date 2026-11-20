-- A graph checkpoint keeps the parts that arrived and wait to be placed: a
-- uuencode part ahead of its file's cursor, or an article that declared no
-- usable start ahead of the ordinal it follows. Each row names the part's
-- spool file, relative to the job's working directory, and its decoded
-- length; a held article keeps the facts its decode found, so a restart
-- places it as it would have been placed. A part held only in memory has no
-- row and is fetched again.
CREATE TABLE graph_held_parts (
    job_id               INTEGER NOT NULL,
    file_index           INTEGER NOT NULL,
    ordinal              INTEGER NOT NULL,
    decoded_len          INTEGER NOT NULL,
    spool_path           TEXT NOT NULL,
    raw_size             INTEGER,
    crc_valid            INTEGER,
    truncation_suspected INTEGER,
    part_crc_verified    INTEGER,
    part_crc             INTEGER,
    expected_file_crc    INTEGER,
    file_size            INTEGER,
    yenc_part            INTEGER,
    yenc_total           INTEGER,
    yenc_begin           INTEGER,
    yenc_end             INTEGER,
    yenc_name            TEXT,
    crc_segments         BLOB,
    PRIMARY KEY (job_id, file_index, ordinal)
) WITHOUT ROWID;
