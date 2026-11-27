-- Why a torrent's unpacking came out short, as a code beside its reason:
-- refused, nothing_delivered, not_placed, delivered_in_part,
-- unwanted_removed or unwanted_not_removed. Null for a whole unpacking,
-- and for every row written before this: nothing to backfill.
ALTER TABLE torrents ADD COLUMN unpack_cause TEXT;
