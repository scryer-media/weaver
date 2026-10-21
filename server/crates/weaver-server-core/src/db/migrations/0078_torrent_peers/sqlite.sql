-- The peers a torrent dials again when it next starts, and the addresses it
-- refuses, each a JSON array of strings; NULL is none.
ALTER TABLE torrents ADD COLUMN peers TEXT;
ALTER TABLE torrents ADD COLUMN banned_peers TEXT;
