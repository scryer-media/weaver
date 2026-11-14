-- The attributes a torrent's job was added with, as JSON pairs; NULL is
-- none.
ALTER TABLE torrents ADD COLUMN metadata TEXT;
