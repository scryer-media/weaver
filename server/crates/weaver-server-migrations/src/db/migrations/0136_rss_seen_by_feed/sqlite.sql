-- Each feed's last seen items, newest first, read without ranking every
-- feed's history: a read of one feed's latest stops at its limit.
CREATE INDEX idx_rss_seen_feed_seen_at ON rss_seen_items (feed_id, seen_at);
