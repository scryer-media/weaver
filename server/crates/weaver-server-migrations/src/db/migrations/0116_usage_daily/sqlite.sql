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
    bytes_downloaded INTEGER NOT NULL DEFAULT 0,
    articles_tried INTEGER NOT NULL DEFAULT 0,
    articles_succeeded INTEGER NOT NULL DEFAULT 0,
    down_seconds INTEGER NOT NULL DEFAULT 0,
    bytes_uploaded INTEGER NOT NULL DEFAULT 0,
    events INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, consumer_kind, consumer_id)
);

-- A removed server whose quota resets on a calendar keeps its rows until its
-- next reset after the removal. Its rows move to the consumer id kept here,
-- which no live server id can take, and go with this row at `remove_at`, in
-- epoch seconds. `host` is the server's name when it was removed.
CREATE TABLE usage_removed_servers (
    consumer_id TEXT PRIMARY KEY,
    server_id INTEGER NOT NULL,
    host TEXT NOT NULL,
    remove_at INTEGER NOT NULL
);
