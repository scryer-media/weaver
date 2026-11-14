-- The history row a torrent's removal could not write for want of room on
-- the database's disk, as JSON, kept on the torrent until it is written; the
-- next start writes it and the torrent leaves. NULL otherwise. It goes with
-- the torrent's row, which goes once its history row is written.
ALTER TABLE torrents ADD COLUMN retiring TEXT;
CREATE INDEX idx_torrents_retiring ON torrents (job_id) WHERE retiring IS NOT NULL;
