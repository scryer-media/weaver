-- Who named a torrent's tracker: the torrent itself (its metainfo or magnet
-- link), or the operator. Every row written before this column is the
-- torrent's: nothing recorded which were added by hand.
ALTER TABLE torrent_trackers
    ADD COLUMN named_by TEXT NOT NULL DEFAULT 'torrent'
    CHECK (named_by IN ('torrent', 'operator'));
