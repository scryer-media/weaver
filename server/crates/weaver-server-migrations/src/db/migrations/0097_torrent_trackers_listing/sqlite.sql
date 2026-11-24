-- A tracker row keeps only where the tracker sits in the torrent's announce
-- list. How announcing to it goes is read from the session, never stored.
CREATE TABLE torrent_trackers_new (
    job_id  INTEGER NOT NULL REFERENCES torrents(job_id) ON DELETE CASCADE,
    tier    INTEGER NOT NULL,
    url     TEXT NOT NULL,
    PRIMARY KEY (job_id, tier, url)
) WITHOUT ROWID;

INSERT INTO torrent_trackers_new (job_id, tier, url)
SELECT job_id, tier, url FROM torrent_trackers;

DROP TABLE torrent_trackers;
ALTER TABLE torrent_trackers_new RENAME TO torrent_trackers;
