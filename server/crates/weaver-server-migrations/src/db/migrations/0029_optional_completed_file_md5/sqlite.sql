CREATE TABLE active_files_new (
    job_id      INTEGER NOT NULL,
    file_index  INTEGER NOT NULL,
    filename    TEXT NOT NULL,
    md5         BLOB,
    PRIMARY KEY (job_id, file_index)
) WITHOUT ROWID;

INSERT INTO active_files_new (job_id, file_index, filename, md5)
SELECT job_id, file_index, filename, md5
FROM active_files;

DROP TABLE active_files;

ALTER TABLE active_files_new RENAME TO active_files;
