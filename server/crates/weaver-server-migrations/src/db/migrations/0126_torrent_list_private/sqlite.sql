-- A torrent list keeps a privacy in the table. Whether a torrent's metainfo
-- marks it private is stored beside the metainfo, false while a magnet has
-- none; the upgrade step reads it from the metainfo of each torrent already
-- kept. The first index walks one privacy's torrents in the order they were
-- added and holds their category, so the torrents in no category are kept
-- from it alone; the second walks one privacy of one category in that order.
ALTER TABLE torrents ADD COLUMN private INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_torrents_private_added
    ON torrents (private, added_at_ms, job_id, category_id);
CREATE INDEX IF NOT EXISTS idx_torrents_category_private_added
    ON torrents (category_id, private, added_at_ms, job_id);
