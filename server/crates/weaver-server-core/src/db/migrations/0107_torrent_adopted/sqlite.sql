-- What was already on disk where a torrent's files go before Weaver wrote
-- any of them, as JSON: the files, by their metainfo names, and the folders,
-- relative to the directory the files sit in. A deletion of the torrent's
-- files leaves these. NULL when nothing was there.
ALTER TABLE torrents ADD COLUMN adopted TEXT;
