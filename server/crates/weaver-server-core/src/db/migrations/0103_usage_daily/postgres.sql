-- Data used per day, one row per local date and consumer: a server by its
-- id, a category by its name, a job stage by its name, a tracker by its
-- host, and the job and torrent totals. Each row holds what was downloaded
-- that day, the articles tried and found, the seconds the consumer was down,
-- what was sent to peers, the jobs or stage runs counted, and the stage or
-- seeding time in milliseconds.
CREATE TABLE usage_daily (
    day TEXT NOT NULL,
    consumer_kind TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    bytes_downloaded BIGINT NOT NULL DEFAULT 0,
    articles_tried BIGINT NOT NULL DEFAULT 0,
    articles_succeeded BIGINT NOT NULL DEFAULT 0,
    down_seconds BIGINT NOT NULL DEFAULT 0,
    bytes_uploaded BIGINT NOT NULL DEFAULT 0,
    events BIGINT NOT NULL DEFAULT 0,
    duration_ms BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, consumer_kind, consumer_id)
);
