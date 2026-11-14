-- A torrent's own upload and download limits in bytes per second; NULL is
-- unlimited and 0 holds that direction closed.
ALTER TABLE torrents ADD COLUMN upload_limit INTEGER;
ALTER TABLE torrents ADD COLUMN download_limit INTEGER;
