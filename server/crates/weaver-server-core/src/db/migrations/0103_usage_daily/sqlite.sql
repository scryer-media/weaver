-- Data used per day, one row per local date and consumer: a server by its
-- id, a category by its name, and any kind added later by its own key. Each
-- row holds what was downloaded that day, the articles tried and found, and
-- the seconds the consumer was down.
CREATE TABLE usage_daily (
    day TEXT NOT NULL,
    consumer_kind TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    bytes_downloaded INTEGER NOT NULL DEFAULT 0,
    articles_tried INTEGER NOT NULL DEFAULT 0,
    articles_succeeded INTEGER NOT NULL DEFAULT 0,
    down_seconds INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, consumer_kind, consumer_id)
);
