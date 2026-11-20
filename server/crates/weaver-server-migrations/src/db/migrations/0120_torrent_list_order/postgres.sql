-- A torrent list reads one page, in the order the torrents were added, every
-- torrent's or a category's. The first index serves the whole list and the
-- second a category's, each in that order, so a page walks only its own rows
-- and is never sorted. The second leads with the category, so it serves
-- every read the category index served, which goes.
CREATE INDEX IF NOT EXISTS idx_torrents_added
    ON torrents (added_at_ms, job_id);
CREATE INDEX IF NOT EXISTS idx_torrents_category_added
    ON torrents (category_id, added_at_ms, job_id);
DROP INDEX IF EXISTS idx_torrents_category;
