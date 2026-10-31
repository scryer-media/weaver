-- A graph checkpoint keeps the parts that arrived and wait to be placed: a
-- uuencode part ahead of its file's cursor, or an article that declared no
-- usable start ahead of the ordinal it follows. Each row names the part's
-- spool file, relative to the job's working directory, and its decoded
-- length; a held article keeps the facts its decode found, so a restart
-- places it as it would have been placed. A part held only in memory has no
-- row and is fetched again.
CREATE TABLE graph_held_parts (
    job_id               BIGINT NOT NULL,
    file_index           BIGINT NOT NULL,
    ordinal              BIGINT NOT NULL,
    decoded_len          BIGINT NOT NULL,
    spool_path           TEXT NOT NULL,
    raw_size             BIGINT,
    crc_valid            BOOLEAN,
    truncation_suspected BOOLEAN,
    part_crc_verified    BOOLEAN,
    part_crc             BIGINT,
    expected_file_crc    BIGINT,
    file_size            BIGINT,
    yenc_part            BIGINT,
    yenc_total           BIGINT,
    yenc_begin           BIGINT,
    yenc_end             BIGINT,
    yenc_name            TEXT,
    crc_segments         BYTEA,
    PRIMARY KEY (job_id, file_index, ordinal)
);
