-- The kept rows of a removed server go by consumer, and the removals that are
-- due go by time. Both deletes, and the reads of one consumer kind, are served
-- by these. Building them reads each table once; nothing to backfill.
CREATE INDEX IF NOT EXISTS idx_usage_daily_consumer
    ON usage_daily (consumer_kind, consumer_id, day);
CREATE INDEX IF NOT EXISTS idx_usage_removed_servers_remove_at
    ON usage_removed_servers (remove_at);
