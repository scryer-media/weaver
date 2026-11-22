-- A tracker rule gets a name it is known by, and names any number of hosts:
-- each pattern a host, or a host under a leading `*.`, held by one rule at
-- most. The old pattern becomes the rule's one host, and its name is the
-- pattern without `*.`, or the pattern itself where a rule for that bare
-- host exists.
ALTER TABLE tracker_rules ADD COLUMN name TEXT;

UPDATE tracker_rules old SET name = CASE
    WHEN old.host_pattern LIKE '*.%' AND NOT EXISTS (
        SELECT 1 FROM tracker_rules bare
         WHERE bare.host_pattern = substr(old.host_pattern, 3)
    ) THEN substr(old.host_pattern, 3)
    ELSE old.host_pattern
END;

ALTER TABLE tracker_rules ALTER COLUMN name SET NOT NULL;

-- Names are unique whatever their case.
CREATE UNIQUE INDEX idx_tracker_rules_name ON tracker_rules (lower(name));

CREATE TABLE tracker_rule_hosts (
    pattern TEXT PRIMARY KEY NOT NULL,
    rule_id BIGINT NOT NULL REFERENCES tracker_rules (id) ON DELETE CASCADE,
    -- Where the pattern stands in its rule's list, the first the one the
    -- rule shows itself by.
    position INTEGER NOT NULL
);
CREATE INDEX idx_tracker_rule_hosts_rule ON tracker_rule_hosts (rule_id, position);

INSERT INTO tracker_rule_hosts (pattern, rule_id, position)
SELECT host_pattern, id, 0 FROM tracker_rules;

ALTER TABLE tracker_rules DROP COLUMN host_pattern;
