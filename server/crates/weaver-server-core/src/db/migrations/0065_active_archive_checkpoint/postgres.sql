CREATE TABLE active_archive_checkpoint (
    job_id BIGINT NOT NULL PRIMARY KEY,
    generation BIGINT NOT NULL CHECK (generation > 0),
    reference BYTEA NOT NULL CHECK (octet_length(reference) BETWEEN 1 AND 256)
);
