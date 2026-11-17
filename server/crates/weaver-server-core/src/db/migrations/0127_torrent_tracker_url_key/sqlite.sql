-- A tracker row is told from another by its URL's keyed digest. The URL is
-- stored encrypted, and its ciphertext differs each time it is written, so
-- the primary key over it no longer keeps one URL from being listed twice in
-- a tier. `url_key` is the URL's HMAC-SHA256 under a key derived from the
-- instance key: the same for the same URL, and no use without that key. It
-- is null on a row written before this, until the start that seals the
-- stored secrets fills it, because no migration holds the instance key; a
-- null is no duplicate of another. The unique index is the row's key from
-- then on, and serves the reads by job as the primary key did.
ALTER TABLE torrent_trackers ADD COLUMN url_key BLOB;
CREATE UNIQUE INDEX IF NOT EXISTS idx_torrent_trackers_url_key
    ON torrent_trackers (job_id, tier, url_key);
