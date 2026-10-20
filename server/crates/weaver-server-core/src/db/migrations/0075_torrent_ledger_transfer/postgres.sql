-- A torrent's ledger keeps the bytes that verified, the time it ran in a
-- session, and the last time a complete copy was seen.
ALTER TABLE torrent_counters ADD COLUMN checked_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE torrent_counters ADD COLUMN active_duration_s BIGINT NOT NULL DEFAULT 0;
ALTER TABLE torrent_counters ADD COLUMN last_seen_complete_ms BIGINT;
