-- A torrent keeps only what its next start needs. The ledger drops the
-- verified bytes (a run's wasted bytes are the session's own), the upload
-- total of the last announce (never written) and the time it was written
-- (the service keeps that in memory). The torrent row drops whether it is
-- private (its metainfo says so) and the tracker rule it matched (its
-- trackers and the rules say so).
ALTER TABLE torrent_counters DROP COLUMN checked_bytes;
ALTER TABLE torrent_counters DROP COLUMN last_announce_uploaded_bytes;
ALTER TABLE torrent_counters DROP COLUMN updated_at_ms;

-- The peers and bans a torrent starts from move to its ledger row, which is
-- written as it runs, off the row that holds its metainfo. Every torrent has
-- that row.
ALTER TABLE torrent_counters ADD COLUMN peers TEXT;
ALTER TABLE torrent_counters ADD COLUMN banned_peers TEXT;
INSERT INTO torrent_counters (job_id)
SELECT job_id FROM torrents
 WHERE NOT EXISTS (SELECT 1 FROM torrent_counters WHERE torrent_counters.job_id = torrents.job_id);
UPDATE torrent_counters
   SET peers = (SELECT peers FROM torrents WHERE torrents.job_id = torrent_counters.job_id),
       banned_peers = (SELECT banned_peers FROM torrents WHERE torrents.job_id = torrent_counters.job_id);

ALTER TABLE torrents DROP COLUMN peers;
ALTER TABLE torrents DROP COLUMN banned_peers;
ALTER TABLE torrents DROP COLUMN private;
ALTER TABLE torrents DROP COLUMN tracker_rule_id;
