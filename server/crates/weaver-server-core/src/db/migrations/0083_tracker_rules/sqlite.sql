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
    -- The tracker file that filled the rule: its name, revision and
    -- import time, the fields it locked and the values it recommended,
    -- as JSON; NULL when no file binds the rule.
    bound          TEXT,
    -- The archive passwords that file carried, sealed with the
    -- database's key; NULL for none.
    archive_passwords TEXT,
    created_at_ms  INTEGER NOT NULL,
    updated_at_ms  INTEGER NOT NULL
);
