-- A torrent's removal under way: set before anything of the torrent goes,
-- to 'kept' when its files stay or 'deleted' when they go with it, so a stop
-- from then on brings it back retiring, never live, and the next start
-- finishes the removal from the row and writes its history. NULL otherwise.
-- It goes with the torrent's row, which goes once its history row is
-- written.
ALTER TABLE torrents ADD COLUMN retiring TEXT;
CREATE INDEX idx_torrents_retiring ON torrents (job_id) WHERE retiring IS NOT NULL;
