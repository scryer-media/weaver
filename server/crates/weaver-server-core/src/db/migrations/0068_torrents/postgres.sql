-- A torrent job: the job id is the same one its queue and history rows carry.
-- At least one infohash is known from the moment the torrent is added.
CREATE TABLE torrents (
    job_id           BIGINT PRIMARY KEY NOT NULL,
    infohash_v1      BYTEA,
    infohash_v2      BYTEA,
    name             TEXT NOT NULL,
    -- magnet or metainfo: what the torrent was added from.
    source           TEXT NOT NULL,
    -- The torrent's metainfo file, once known.
    metainfo         BYTEA,
    private          BOOLEAN NOT NULL DEFAULT FALSE,
    category_id      BIGINT,
    added_at_ms      BIGINT NOT NULL,
    state            TEXT NOT NULL,
    forced           BOOLEAN NOT NULL DEFAULT FALSE,
    completed_at_ms  BIGINT,
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
    job_id      BIGINT NOT NULL REFERENCES torrents(job_id) ON DELETE CASCADE,
    file_index  BIGINT NOT NULL,
    path        TEXT NOT NULL,
    length      BIGINT NOT NULL,
    selected    BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (job_id, file_index)
);

CREATE TABLE torrent_trackers (
    job_id            BIGINT NOT NULL REFERENCES torrents(job_id) ON DELETE CASCADE,
    tier              BIGINT NOT NULL,
    url               TEXT NOT NULL,
    status            TEXT NOT NULL,
    last_announce_ms  BIGINT,
    next_announce_ms  BIGINT,
    message           TEXT,
    seeders           BIGINT,
    leechers          BIGINT,
    PRIMARY KEY (job_id, tier, url)
);

-- The seeding ledger: what a torrent has moved, kept across restarts.
CREATE TABLE torrent_counters (
    job_id                        BIGINT PRIMARY KEY NOT NULL REFERENCES torrents(job_id) ON DELETE CASCADE,
    uploaded_bytes                BIGINT NOT NULL DEFAULT 0,
    downloaded_bytes              BIGINT NOT NULL DEFAULT 0,
    finished_duration_s           BIGINT NOT NULL DEFAULT 0,
    last_activity_ms              BIGINT,
    last_announce_uploaded_bytes  BIGINT NOT NULL DEFAULT 0,
    updated_at_ms                 BIGINT NOT NULL
);

-- A category's seeding policy, as JSON, and whether its torrents route
-- through their own consumer.
ALTER TABLE categories ADD COLUMN seeding TEXT;
ALTER TABLE categories ADD COLUMN torrent_route BOOLEAN NOT NULL DEFAULT FALSE;

-- What a finished job was downloaded over. Every job before this migration
-- was an NZB.
ALTER TABLE job_history ADD COLUMN protocol TEXT NOT NULL DEFAULT 'nzb';
