-- A torrent keeps only what its next start needs. The ledger drops the
-- verified bytes, the upload total of the last announce (never written) and
-- the time it was written (the service keeps that in memory). The torrent
-- row drops whether it is private (its metainfo says so).
ALTER TABLE torrent_counters
    DROP COLUMN checked_bytes,
    DROP COLUMN last_announce_uploaded_bytes,
    DROP COLUMN updated_at_ms;

-- The bytes a torrent fetched and threw away, kept across restarts: those of
-- pieces that failed their check, and those of blocks that arrived when
-- already in hand.
ALTER TABLE torrent_counters ADD COLUMN failed_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE torrent_counters ADD COLUMN redundant_bytes BIGINT NOT NULL DEFAULT 0;

-- The peers and bans a torrent starts from move to its ledger row, which is
-- written as it runs, off the row that holds its metainfo. Every torrent has
-- that row.
ALTER TABLE torrent_counters ADD COLUMN peers TEXT;
ALTER TABLE torrent_counters ADD COLUMN banned_peers TEXT;
INSERT INTO torrent_counters (job_id)
SELECT job_id FROM torrents
 WHERE NOT EXISTS (SELECT 1 FROM torrent_counters WHERE torrent_counters.job_id = torrents.job_id);
UPDATE torrent_counters
   SET peers = torrents.peers,
       banned_peers = torrents.banned_peers
  FROM torrents
 WHERE torrents.job_id = torrent_counters.job_id;

ALTER TABLE torrents
    DROP COLUMN peers,
    DROP COLUMN banned_peers,
    DROP COLUMN private;
