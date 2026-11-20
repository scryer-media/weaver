-- The pieces of a torrent in seed mode that passed their check, a bitfield
-- with the first piece in the high bit of the first byte; NULL when none is
-- kept.
ALTER TABLE torrents ADD COLUMN seed_verified BLOB;
