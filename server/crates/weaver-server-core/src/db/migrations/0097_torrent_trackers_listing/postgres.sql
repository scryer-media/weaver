-- A tracker row keeps only where the tracker sits in the torrent's announce
-- list. How announcing to it goes is read from the session, never stored.
ALTER TABLE torrent_trackers
    DROP COLUMN status,
    DROP COLUMN last_announce_ms,
    DROP COLUMN next_announce_ms,
    DROP COLUMN message,
    DROP COLUMN seeders,
    DROP COLUMN leechers;
