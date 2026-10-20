-- A tracker rule: the seeding goals, and whether the schedule's torrent
-- pause may stop a torrent while those goals are unmet, for every torrent
-- that announces to a host the pattern names. The pattern is a host, or a
-- host under a leading `*.`.
CREATE TABLE tracker_rules (
    id             INTEGER PRIMARY KEY NOT NULL,
    host_pattern   TEXT NOT NULL UNIQUE,
    -- A seeding policy, as JSON, in the shape a category's takes.
    seeding        TEXT,
    never_pause    INTEGER NOT NULL DEFAULT 0,
    created_at_ms  INTEGER NOT NULL,
    updated_at_ms  INTEGER NOT NULL
);

-- The rule a torrent's trackers matched, if any.
ALTER TABLE torrents ADD COLUMN tracker_rule_id INTEGER REFERENCES tracker_rules(id) ON DELETE SET NULL;
