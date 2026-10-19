-- A torrent job: the job id is the same one its queue and history rows carry.
-- At least one infohash is known from the moment the torrent is added.
CREATE TABLE torrents (
    job_id           INTEGER PRIMARY KEY NOT NULL,
    infohash_v1      BLOB,
    infohash_v2      BLOB,
    name             TEXT NOT NULL,
    -- magnet or metainfo: what the torrent was added from.
    source           TEXT NOT NULL,
    -- The torrent's metainfo file, once known.
    metainfo         BLOB,
    private          INTEGER NOT NULL DEFAULT 0,
    category_id      INTEGER,
    added_at_ms      INTEGER NOT NULL,
    state            TEXT NOT NULL,
    forced           INTEGER NOT NULL DEFAULT 0,
    completed_at_ms  INTEGER,
    storage_dir      TEXT,
    -- A seeding policy, as JSON, that overrides the category's and the
    -- global one field by field.
    policy_override  TEXT,
    CHECK (infohash_v1 IS NOT NULL OR infohash_v2 IS NOT NULL)
);

-- One torrent per infohash: its v1 hash, or its full v2 hash when it has no
-- v1 hash.
CREATE UNIQUE INDEX uq_torrents_infohash_v1
    ON torrents (infohash_v1)
    WHERE infohash_v1 IS NOT NULL;
CREATE UNIQUE INDEX uq_torrents_infohash_v2
    ON torrents (infohash_v2)
    WHERE infohash_v1 IS NULL;
CREATE INDEX idx_torrents_category ON torrents (category_id);

CREATE TABLE torrent_files (
    job_id      INTEGER NOT NULL REFERENCES torrents(job_id) ON DELETE CASCADE,
    file_index  INTEGER NOT NULL,
    path        TEXT NOT NULL,
    length      INTEGER NOT NULL,
    selected    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (job_id, file_index)
) WITHOUT ROWID;

CREATE TABLE torrent_trackers (
    job_id            INTEGER NOT NULL REFERENCES torrents(job_id) ON DELETE CASCADE,
    tier              INTEGER NOT NULL,
    url               TEXT NOT NULL,
    status            TEXT NOT NULL,
    last_announce_ms  INTEGER,
    next_announce_ms  INTEGER,
    message           TEXT,
    seeders           INTEGER,
    leechers          INTEGER,
    PRIMARY KEY (job_id, tier, url)
) WITHOUT ROWID;

-- The seeding ledger: what a torrent has moved, kept across restarts.
CREATE TABLE torrent_counters (
    job_id                        INTEGER PRIMARY KEY NOT NULL REFERENCES torrents(job_id) ON DELETE CASCADE,
    uploaded_bytes                INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes              INTEGER NOT NULL DEFAULT 0,
    finished_duration_s           INTEGER NOT NULL DEFAULT 0,
    last_activity_ms              INTEGER,
    last_announce_uploaded_bytes  INTEGER NOT NULL DEFAULT 0,
    updated_at_ms                 INTEGER NOT NULL
);

-- A category's seeding policy, as JSON, and whether its torrents route
-- through their own consumer.
ALTER TABLE categories ADD COLUMN seeding TEXT;
ALTER TABLE categories ADD COLUMN torrent_route INTEGER NOT NULL DEFAULT 0;

-- What a finished job was downloaded over. Every job before this migration
-- was an NZB.
ALTER TABLE job_history ADD COLUMN protocol TEXT NOT NULL DEFAULT 'nzb';
