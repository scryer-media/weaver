-- Whether a torrent's unpacking is over, whole or short: set in the write
-- that places what it unpacked, or where it ends short. A torrent reopened
-- after it makes no tee, so it does not unpack again. False for every row
-- written before this: torrents have not shipped, so nothing to backfill.
ALTER TABLE torrents ADD COLUMN unpack_over BOOLEAN NOT NULL DEFAULT FALSE;
