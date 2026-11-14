-- One torrent per infohash: a torrent's v2 hash is its own wherever it is
-- known, hybrids included, so a hybrid added by either hash is one torrent.
DROP INDEX uq_torrents_infohash_v2;
CREATE UNIQUE INDEX uq_torrents_infohash_v2
    ON torrents (infohash_v2)
    WHERE infohash_v2 IS NOT NULL;
