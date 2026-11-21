-- Planned reconstructed files have no NZB articles. This manifest records
-- ownership and layout length, never trusted verification evidence.
CREATE TABLE active_repair_outputs (
    job_id BIGINT NOT NULL REFERENCES active_jobs(job_id) ON DELETE CASCADE,
    file_index BIGINT NOT NULL CHECK (file_index >= 0 AND file_index <= 4294967295),
    filename TEXT NOT NULL,
    expected_length BIGINT NOT NULL CHECK (expected_length >= 0),
    PRIMARY KEY (job_id, file_index),
    UNIQUE (job_id, filename)
);
