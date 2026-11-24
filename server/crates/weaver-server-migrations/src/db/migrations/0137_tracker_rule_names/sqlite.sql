-- A tracker rule gets a name it is known by, and names any number of hosts:
-- each pattern a host, or a host under a leading `*.`, held by one rule at
-- most. The old pattern becomes the rule's one host, and its name is the
-- pattern without `*.`, or the pattern itself where a rule for that bare
-- host exists. SQLite drops a UNIQUE column only by rebuilding the table;
-- no table refers to it.
ALTER TABLE tracker_rules RENAME TO tracker_rules_v96;

CREATE TABLE tracker_rules (
    id             INTEGER PRIMARY KEY NOT NULL,
    name           TEXT NOT NULL,
    seeding        TEXT,
    never_pause    INTEGER NOT NULL DEFAULT 0,
    bound          TEXT,
    archive_passwords TEXT,
    created_at_ms  INTEGER NOT NULL,
    updated_at_ms  INTEGER NOT NULL,
    unpack         INTEGER,
    client         TEXT,
    torrent_route  INTEGER NOT NULL DEFAULT 0,
    dht            INTEGER,
    pex            INTEGER,
    lsd            INTEGER,
    encryption     TEXT,
    upload_cap     TEXT,
    forced         INTEGER NOT NULL DEFAULT 0,
    ssl_certificate TEXT,
    ssl_private_key TEXT,
    peer_tcp       INTEGER,
    peer_utp       INTEGER,
    prefer_downloading TEXT,
    prefer_seeding TEXT
);

INSERT INTO tracker_rules (
    id, name, seeding, never_pause, bound, archive_passwords, created_at_ms,
    updated_at_ms, unpack, client, torrent_route, dht, pex, lsd, encryption,
    upload_cap, forced, ssl_certificate, ssl_private_key, peer_tcp, peer_utp,
    prefer_downloading, prefer_seeding
)
SELECT
    old.id,
    CASE
        WHEN old.host_pattern LIKE '*.%' AND NOT EXISTS (
            SELECT 1 FROM tracker_rules_v96 bare
             WHERE bare.host_pattern = substr(old.host_pattern, 3)
        ) THEN substr(old.host_pattern, 3)
        ELSE old.host_pattern
    END,
    old.seeding, old.never_pause, old.bound, old.archive_passwords,
    old.created_at_ms, old.updated_at_ms, old.unpack, old.client,
    old.torrent_route, old.dht, old.pex, old.lsd, old.encryption,
    old.upload_cap, old.forced, old.ssl_certificate, old.ssl_private_key,
    old.peer_tcp, old.peer_utp, old.prefer_downloading, old.prefer_seeding
FROM tracker_rules_v96 old;

-- Names are unique whatever their case.
CREATE UNIQUE INDEX idx_tracker_rules_name ON tracker_rules (lower(name));

CREATE TABLE tracker_rule_hosts (
    pattern TEXT PRIMARY KEY NOT NULL,
    rule_id INTEGER NOT NULL REFERENCES tracker_rules (id) ON DELETE CASCADE,
    -- Where the pattern stands in its rule's list, the first the one the
    -- rule shows itself by.
    position INTEGER NOT NULL
);
CREATE INDEX idx_tracker_rule_hosts_rule ON tracker_rule_hosts (rule_id, position);

INSERT INTO tracker_rule_hosts (pattern, rule_id, position)
SELECT host_pattern, id, 0 FROM tracker_rules_v96;

DROP TABLE tracker_rules_v96;
