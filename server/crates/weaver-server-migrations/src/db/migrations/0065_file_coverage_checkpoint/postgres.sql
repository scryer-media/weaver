-- A file's restart checkpoint is its written coverage, not a contiguous
-- prefix: the runs of articles on disk, wherever in the file they sit, as one
-- versioned blob replaced at every barrier.
--
-- A prefix floor is a count of decoded bytes, and reading it back as articles
-- needs the NZB, so it cannot be converted here. Its rows are dropped with the
-- table: a file mid-download across the upgrade downloads again from
-- nothing, which is what a refused checkpoint costs.
DROP TABLE active_file_progress;

CREATE TABLE active_file_progress (
    job_id      BIGINT NOT NULL,
    file_index  BIGINT NOT NULL,
    coverage    BYTEA NOT NULL,
    PRIMARY KEY (job_id, file_index)
);
