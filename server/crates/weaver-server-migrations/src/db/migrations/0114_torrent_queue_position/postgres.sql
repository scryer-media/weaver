-- Where a torrent stands in the download queue, the lowest first. The
-- torrents already kept take the order they were added in.
ALTER TABLE torrents ADD COLUMN queue_position BIGINT NOT NULL DEFAULT 0;
UPDATE torrents SET queue_position = (
    SELECT COUNT(*) FROM torrents AS earlier
    WHERE earlier.added_at_ms < torrents.added_at_ms
        OR (earlier.added_at_ms = torrents.added_at_ms AND earlier.job_id < torrents.job_id)
);
