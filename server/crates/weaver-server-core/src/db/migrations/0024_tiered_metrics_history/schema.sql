CREATE TABLE IF NOT EXISTS metrics_history_chunks (
    resolution_sec        INTEGER NOT NULL,
    chunk_start_epoch_sec INTEGER NOT NULL,
    body_zstd             BLOB NOT NULL,
    PRIMARY KEY (resolution_sec, chunk_start_epoch_sec)
) WITHOUT ROWID;

DROP TABLE IF EXISTS metrics_scrapes;
