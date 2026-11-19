-- How a torrent's unpacking came out, once it is over: whole, part,
-- failed or refused, and why when it is short. It is the unpacking's own
-- line on the torrent's detail and never the torrent's status; a restart
-- has no tee to ask again, so the row keeps it. Null for every row
-- written before this: torrents have not shipped, so nothing to backfill.
ALTER TABLE torrents ADD COLUMN unpack_end TEXT;
ALTER TABLE torrents ADD COLUMN unpack_why TEXT;
